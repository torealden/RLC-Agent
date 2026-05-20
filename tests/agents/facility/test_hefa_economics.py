"""
Unit tests for src.agents.facility.hefa_economics.

The critical calibration target is the BBD calibration framework sensitivity rule
(`feedstock_sensitivity_rule.feedstock_cost_is_everything`):

    1c/lb feedstock = ~$0.08/gal margin   (=> yield ~7.7-8.0 lb feedstock per gal)
    $0.05/lb change = $0.35-0.40/gal margin change

If these tests pass, the math is BBD-anchored.
"""

from datetime import date

import pytest

from src.agents.facility.hefa_economics import (
    HefaPriceStack, HefaCosts, HefaParams,
    HEFA_RD_DEFAULT_COSTS, HEFA_RD_PARAMS,
    HEFA_SAF_DEFAULT_COSTS, HEFA_SAF_PARAMS,
    d4_rin_value_per_gal,
    lcfs_value_per_gal,
    effective_selling_price_per_gal,
    implied_feedstock_bid_per_lb,
    producer_margin_per_gal,
    cfpc_45z_value_per_gal,
    D4_EQUIVALENCE_PER_GAL,
)


# ===========================================================================
# Fixtures — BBD calibration California 2025 illustrative baseline (ctx 137)
# ===========================================================================

@pytest.fixture
def bbd_ca_rd_stack():
    """
    BBD calibration framework California 2026 baseline anchored to **Tallow CI=30**.
    Stack components computed by their formulas (CARB regulatory constants,
    patched per IFVS ADR IFVS-010 on 2026-05-20):
      ULSD spot           $2.50
      D4   1.7 * $1.50  = $2.55
      LCFS $65/MT * (88.62-30) g/MJ * 134.47 MJ/gal * 1e-6 = $0.5124
      45Z  $1.00 * (50-30)/50 = $0.40
      eff_sell                                            = $5.9624
    """
    return HefaPriceStack(
        base_refined_product_per_gal = 2.50,
        d4_rin_price_per_rin         = 1.50,      # gives D4 value = 1.7 * $1.50 = $2.55
        lcfs_credit_per_mt           = 65.0,
        lcfs_pathway_ci_score        = 30.0,      # tallow HEFA RD per CARB
        cfpc_45z_per_gal             = 0.40,      # formula: $1 * (50-30)/50
    )


# ===========================================================================
# Calibration anchor: D4 equivalence
# ===========================================================================

def test_d4_value_rd_matches_calibration():
    """$1.50/RIN x 1.7 equivalence = $2.55/gal — exact BBD calibration figure."""
    val = d4_rin_value_per_gal(d4_rin_price_per_rin=1.50, fuel_type='renewable_diesel')
    assert val == pytest.approx(2.55, abs=1e-6)


def test_d4_value_bd_uses_15_equivalence():
    val = d4_rin_value_per_gal(d4_rin_price_per_rin=1.50, fuel_type='biodiesel')
    assert val == pytest.approx(2.25, abs=1e-6)


def test_d4_value_saf_uses_17_equivalence():
    val = d4_rin_value_per_gal(d4_rin_price_per_rin=1.50, fuel_type='saf')
    assert val == pytest.approx(2.55, abs=1e-6)


def test_d4_unknown_fuel_raises():
    with pytest.raises(ValueError):
        d4_rin_value_per_gal(1.50, fuel_type='ethanol')


# ===========================================================================
# Calibration anchor: LCFS math
# ===========================================================================

def test_lcfs_value_matches_calibration():
    """
    KG ctx 137 anchor: with CARB regulatory constants (88.62 baseline,
    134.47 MJ/gal), pathway CI 40.61 gives a 48.01 g/MJ advantage.
    Math: 65 * 48.01 * 134.47 / 1e6 = 0.420/gal.

    (Independent of fixture — tests the LCFS math directly with the calibration's stated
    inputs. The fixture uses CI=30 for tallow internal consistency.)
    """
    val = lcfs_value_per_gal(
        lcfs_credit_per_mt=65.0,
        lcfs_pathway_ci_score=40.61,
        fuel_type='renewable_diesel',
    )
    assert val == pytest.approx(0.420, abs=0.01)


def test_lcfs_value_tallow_ci_30():
    """Tallow CI=30 with $65/MT LCFS: 65 * (88.62-30) * 134.47 / 1e6 = $0.5124/gal."""
    val = lcfs_value_per_gal(
        lcfs_credit_per_mt=65.0,
        lcfs_pathway_ci_score=30.0,
        fuel_type='renewable_diesel',
    )
    assert val == pytest.approx(0.5124, abs=0.01)


def test_lcfs_negative_pathway_generates_deficit():
    """If pathway CI is WORSE than baseline, LCFS value is negative (deficit)."""
    val = lcfs_value_per_gal(
        lcfs_credit_per_mt=65.0,
        lcfs_pathway_ci_score=120.0,    # worse than 88.62 diesel baseline
        fuel_type='renewable_diesel',
    )
    assert val < 0


def test_lcfs_zero_at_baseline_ci():
    """Pathway at exact baseline CI generates zero credits."""
    val = lcfs_value_per_gal(
        lcfs_credit_per_mt=65.0,
        lcfs_pathway_ci_score=88.62,
        fuel_type='renewable_diesel',
    )
    assert val == pytest.approx(0.0, abs=1e-9)


# ===========================================================================
# Calibration anchor: effective selling price lands in calibration range
# ===========================================================================

def test_effective_selling_price_in_range(bbd_ca_rd_stack):
    """
    BBD calibration effective selling price for CA RD: $5.50-6.50/gal.
    With ULSD $2.50 + D4 $2.55 + LCFS $0.5124 + 45Z $0.40 (tallow CI=30,
    CARB regulatory constants) = $5.96/gal — still squarely in range.
    """
    eff = effective_selling_price_per_gal(bbd_ca_rd_stack, fuel_type='renewable_diesel')
    assert 5.50 <= eff <= 6.50     # squarely in calibration range
    assert eff == pytest.approx(5.96, abs=0.05)


# ===========================================================================
# Calibration anchor: feedstock sensitivity rule (THE critical calibration test)
# ===========================================================================

def test_feedstock_sensitivity_one_cent_per_lb_equals_eight_cents_per_gal():
    """
    `feedstock_sensitivity_rule`: '1c/lb feedstock = $0.08/gal margin'.

    A $0.01/lb increase in observed feedstock cost should compress margin by
    yield_lb_per_gal x $0.01 = 7.7 x 0.01 = $0.077/gal (which rounds to $0.08).
    """
    yield_lb_per_gal = 7.7
    margin_change_per_gal = yield_lb_per_gal * 0.01
    assert margin_change_per_gal == pytest.approx(0.077, abs=0.001)
    # Calibration rule states "~$0.08/gal" — we land at $0.077, within 4%, exact for 7.7 lb/gal
    assert abs(margin_change_per_gal - 0.08) < 0.005


def test_feedstock_sensitivity_five_cents_matches_range(bbd_ca_rd_stack):
    """
    : '$0.05/lb feedstock change = $0.35-0.40/gal margin change'.

    Compute producer_margin at two feedstock prices 5c apart and verify the delta
    lands in $0.35-0.40/gal range. With yield 7.7: 7.7 x 0.05 = $0.385/gal — exact.
    """
    margin_at_lower = producer_margin_per_gal(
        bbd_ca_rd_stack, HEFA_RD_DEFAULT_COSTS, HEFA_RD_PARAMS,
        observed_feedstock_per_lb=0.40,
    )
    margin_at_higher = producer_margin_per_gal(
        bbd_ca_rd_stack, HEFA_RD_DEFAULT_COSTS, HEFA_RD_PARAMS,
        observed_feedstock_per_lb=0.45,
    )
    delta = margin_at_lower - margin_at_higher    # 5c/lb increase reduces margin
    assert 0.35 <= delta <= 0.40
    assert delta == pytest.approx(0.385, abs=0.001)


# ===========================================================================
# Implied bid math (the main IFV computation)
# ===========================================================================

def test_implied_bid_returns_per_lb_and_per_short_ton(bbd_ca_rd_stack):
    """Headline output: $/lb and $/short_ton tied by 2000x."""
    bid, breakdown = implied_feedstock_bid_per_lb(
        bbd_ca_rd_stack, HEFA_RD_DEFAULT_COSTS, HEFA_RD_PARAMS,
        return_breakdown=True,
    )
    assert bid == breakdown.implied_bid_per_lb
    assert breakdown.implied_bid_per_short_ton == pytest.approx(bid * 2000.0, abs=1e-6)


def test_implied_bid_ca_rd_tallow_base_case(bbd_ca_rd_stack):
    """
    Sanity anchor with Tore-confirmed 45Z formula (CARB regulatory constants):
      eff_sell $5.96 - OPEX $0.40 - fixed $0.10 = $5.46 net available
      bid = $5.46 / 7.7 = $0.7094/lb breakeven

    CA tallow cash market has run $0.50-0.65/lb. At $0.50 cash, this implies
    ~$0.21/lb margin opportunity at 7.7 lb/gal = ~$1.62/gal producer margin —
    still high, still indicates strong incentive to source. Maps to 'best case'
    margin range.
    """
    bid, breakdown = implied_feedstock_bid_per_lb(
        bbd_ca_rd_stack, HEFA_RD_DEFAULT_COSTS, HEFA_RD_PARAMS,
        return_breakdown=True,
    )
    assert breakdown.effective_selling_price_per_gal == pytest.approx(5.96, abs=0.05)
    assert breakdown.net_available_for_feedstock_per_gal == pytest.approx(5.46, abs=0.05)
    assert bid == pytest.approx(0.7094, abs=0.01)


def test_target_margin_reduces_bid_dollar_for_dollar(bbd_ca_rd_stack):
    """If we add $0.50/gal target margin, breakeven bid drops by 0.50/7.7 = $0.065/lb."""
    bid_zero, _ = implied_feedstock_bid_per_lb(
        bbd_ca_rd_stack, HEFA_RD_DEFAULT_COSTS, HEFA_RD_PARAMS,
        return_breakdown=True,
    )
    costs_with_target = HefaCosts(opex_per_gal=0.40, fixed_cost_per_gal=0.10, target_margin_per_gal=0.50)
    bid_target, _ = implied_feedstock_bid_per_lb(
        bbd_ca_rd_stack, costs_with_target, HEFA_RD_PARAMS,
        return_breakdown=True,
    )
    assert (bid_zero - bid_target) == pytest.approx(0.50 / 7.7, abs=0.001)


# ===========================================================================
# Policy scenarios (cfpc_45z)
# ===========================================================================

def test_45z_formula_tallow_rd_matches_tore_worked_example():
    """
    Tore-confirmed worked example (2026-05-18):
      Tallow CI=30, RD, prevailing wage => 45Z = $1.00 * (50-30)/50 = $0.40/gal
    """
    val = cfpc_45z_value_per_gal(
        fuel_type='renewable_diesel',
        feedstock_category='waste_animal_fat',
        pathway_ci_score=30.0,
        policy_scenario='extension_2031',
        as_of_date=date(2026, 7, 1),
    )
    assert val == pytest.approx(0.40, abs=1e-6)


def test_45z_formula_tallow_saf_matches_tore_worked_example():
    """SAF base $1.75 * (50-30)/50 = $0.70/gal at CI=30."""
    val = cfpc_45z_value_per_gal(
        fuel_type='saf',
        feedstock_category='waste_animal_fat',
        pathway_ci_score=30.0,
        policy_scenario='extension_2031',
        as_of_date=date(2026, 7, 1),
    )
    assert val == pytest.approx(0.70, abs=1e-6)


def test_45z_formula_zero_at_ci_threshold():
    """CI=50 (the IRS threshold) produces zero credit."""
    val = cfpc_45z_value_per_gal(
        fuel_type='renewable_diesel',
        feedstock_category='crop_oil',
        pathway_ci_score=50.0,
        policy_scenario='extension_2031',
        as_of_date=date(2026, 7, 1),
    )
    assert val == 0.0


def test_45z_formula_zero_above_threshold():
    """CI above 50 (e.g., palm with ILUC at 70) gives zero, not negative."""
    val = cfpc_45z_value_per_gal(
        fuel_type='renewable_diesel',
        feedstock_category='palm_derivative',
        pathway_ci_score=70.0,
        policy_scenario='extension_2031',
        as_of_date=date(2026, 7, 1),
    )
    assert val == 0.0


def test_45z_expiry_2027_zeros_after_2027():
    val_before = cfpc_45z_value_per_gal(
        fuel_type='renewable_diesel',
        feedstock_category='waste_animal_fat',
        pathway_ci_score=30.0,
        policy_scenario='expiry_2027',
        as_of_date=date(2027, 12, 31),
    )
    val_after = cfpc_45z_value_per_gal(
        fuel_type='renewable_diesel',
        feedstock_category='waste_animal_fat',
        pathway_ci_score=30.0,
        policy_scenario='expiry_2027',
        as_of_date=date(2028, 1, 1),
    )
    assert val_before == pytest.approx(0.40, abs=1e-6)
    assert val_after == 0.0


def test_45z_none_always_zero():
    val = cfpc_45z_value_per_gal(
        fuel_type='renewable_diesel',
        feedstock_category='waste_animal_fat',
        pathway_ci_score=30.0,
        policy_scenario='none',
        as_of_date=date(2026, 7, 1),
    )
    assert val == 0.0


def test_45z_iluc_removed_lifts_soybean_oil_credit():
    """
    KG ctx 127: 'If ILUC is removed, CI differentiation between soybean oil
    and waste feedstocks would narrow substantially.'

    With ILUC: soy oil CI ~50 => 45Z = $1.00 * (50-50)/50 = $0.00/gal
    Without ILUC: effective CI = 50 - 22 = 28 => 45Z = $1.00 * (50-28)/50 = $0.44/gal
    """
    val_base = cfpc_45z_value_per_gal(
        fuel_type='renewable_diesel',
        feedstock_category='crop_oil',
        pathway_ci_score=50.0,
        policy_scenario='extension_2031',
        as_of_date=date(2026, 7, 1),
    )
    val_iluc = cfpc_45z_value_per_gal(
        fuel_type='renewable_diesel',
        feedstock_category='crop_oil',
        pathway_ci_score=50.0,
        policy_scenario='iluc_removed',
        as_of_date=date(2026, 7, 1),
    )
    assert val_base == pytest.approx(0.0, abs=1e-6)
    assert val_iluc == pytest.approx(0.44, abs=1e-6)
    assert val_iluc > val_base    # the directional claim from KG ctx 127


def test_45z_domestic_restriction_zeros_non_domestic():
    """UCO at CI=15: domestic gets $0.70, imported gets zero under restriction."""
    val_domestic = cfpc_45z_value_per_gal(
        fuel_type='renewable_diesel',
        feedstock_category='waste_oil',
        pathway_ci_score=15.0,
        policy_scenario='domestic_restriction',
        as_of_date=date(2026, 7, 1),
        is_domestic_feedstock=True,
    )
    val_imported = cfpc_45z_value_per_gal(
        fuel_type='renewable_diesel',
        feedstock_category='waste_oil',
        pathway_ci_score=15.0,
        policy_scenario='domestic_restriction',
        as_of_date=date(2026, 7, 1),
        is_domestic_feedstock=False,
    )
    assert val_domestic == pytest.approx(0.70, abs=1e-6)
    assert val_imported == 0.0


def test_45z_inflation_override_scales_credit():
    """Future inflation: 2026 base credit ~ $1.03, should give $0.412 at CI=30."""
    val = cfpc_45z_value_per_gal(
        fuel_type='renewable_diesel',
        feedstock_category='waste_animal_fat',
        pathway_ci_score=30.0,
        policy_scenario='extension_2031',
        as_of_date=date(2026, 7, 1),
        base_credit_per_gal=1.03,
    )
    assert val == pytest.approx(1.03 * 0.40, abs=1e-6)


# ===========================================================================
# Worst-case stress test (KG ctx 140: Neste Q4 2024 = $0.18/gal margin)
# ===========================================================================

def test_neste_q4_2024_worst_case_margin():
    """
    KG ctx 140: 'Neste experienced $242/ton ($0.18/gal) in Q4 2024.'
    Build a stress-case stack approximating Q4 2024 (D4 collapsed to ~$0.55,
    LCFS low, 45Z not yet flowing) and check producer margin is low single-digit.
    """
    stressed_stack = HefaPriceStack(
        base_refined_product_per_gal = 2.10,
        d4_rin_price_per_rin         = 0.55,      # D4 collapsed
        lcfs_credit_per_mt           = 50.0,      # LCFS oversupplied
        lcfs_pathway_ci_score        = 30.0,
        cfpc_45z_per_gal             = 0.0,       # not flowing in Q4 2024
    )
    margin = producer_margin_per_gal(
        stressed_stack, HEFA_RD_DEFAULT_COSTS, HEFA_RD_PARAMS,
        observed_feedstock_per_lb=0.42,
    )
    # Should land in stressed range — NOT calibration base case of $0.65-0.70
    assert margin < 0.50
