"""Unit tests for the curve engine's pure logic (no DB).

The transactional tie-out itself is enforced by the deferred trigger from migration 158 and was
proven live against a deliberately broken term (see handoff 2026-08-03); these tests cover the
deterministic assembly logic the trigger cannot see: contract-code parsing and stack arithmetic.
"""

from src.curves.engine import contract_tenor_to_window
from src.curves.specs import BRSBO_FOB_PARITY


class TestContractTenorToWindow:
    def test_standard_codes(self):
        assert contract_tenor_to_window("ZL_U26") == "2026-09"
        assert contract_tenor_to_window("ZL_F27") == "2027-01"
        assert contract_tenor_to_window("ZC_Z26") == "2026-12"
        assert contract_tenor_to_window("MWE_H27") == "2027-03"

    def test_century_pivot(self):
        assert contract_tenor_to_window("ZL_Z99") == "1999-12"
        assert contract_tenor_to_window("ZL_Z69") == "2069-12"

    def test_rejects_front_and_garbage(self):
        assert contract_tenor_to_window("ZL_FRONT") is None
        assert contract_tenor_to_window("FRONT") is None
        assert contract_tenor_to_window("ZL_A26") is None  # A is not a month code
        assert contract_tenor_to_window("ZL_U2") is None
        assert contract_tenor_to_window("ZL_U266") is None


class TestBrsboSpec:
    def test_unit_factor_is_cents_lb_to_usd_per_tonne(self):
        # 100 cents/lb should be 2204.6226 USD/t
        assert abs(100 * BRSBO_FOB_PARITY.unit_factor - 2204.6226) < 1e-6

    def test_stack_sums_board_plus_fixed_terms(self):
        settle_cents_lb = 67.47  # ZL_U26 settle 2026-07-31
        board = settle_cents_lb * BRSBO_FOB_PARITY.unit_factor
        headline = board + sum(t.value for t in BRSBO_FOB_PARITY.fixed_terms)
        assert abs(headline - 1487.458867) < 1e-4

    def test_curve_key_is_tieout_linkage(self):
        # The tie-out finds the headline via series_key == curve_key; a spec whose key
        # drifts from this convention silently loses enforcement.
        assert BRSBO_FOB_PARITY.curve_key == "BRSBO_FOB_PARITY"
        assert BRSBO_FOB_PARITY.quality_rank.startswith("DERIVED_")
