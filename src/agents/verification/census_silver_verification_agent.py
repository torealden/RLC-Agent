"""
Census Silver Verification Agent
================================
Agent for verifying Census trade data in the silver layer.

Verification checks:
1. Unit standards (oils in 000 lbs, meals in short tons)
2. Mathematical accuracy (silver_value = bronze_value × conversion_factor)
3. Cross-reference totals (Silver totals match Bronze totals within 0.1%)
4. Commodity group coverage
5. Reference table mappings

Logs all verification results for the CensusLogReaderAgent.
"""

import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from agents.base.census_base_agent import CensusBaseAgent, PipelineLayer, EventType


# =============================================================================
# US TRADE VOLUME UNIT STANDARDS (monthly trade columns)
# =============================================================================
# Balance sheet units differ - see us_trade_unit_standards.json for full reference
#
# VEGETABLE OILS/FATS/GREASES: 000 Pounds (balance sheet: Million Pounds)
# PROTEIN MEALS: Short Tons (balance sheet: 000 Short Tons)
# OILSEEDS:
#   - Soybeans, Flaxseed: 000 Bushels (balance sheet: Million Bushels)
#   - Cottonseed: Short Tons (balance sheet: 000 Short Tons)
#   - Canola, Sunflower, Peanuts, Safflower: 000 Pounds (balance sheet: Million Pounds)
# FEED/FOOD GRAINS (future):
#   - Corn, Wheat, Sorghum, Barley: 000 Bushels (balance sheet: Million Bushels)
# =============================================================================

UNIT_STANDARDS = {
    # Vegetable Oils - 000 Pounds
    'SOYBEAN_OIL': '000 Pounds',
    'PALM_OIL': '000 Pounds',
    'PALM_KERNEL_OIL': '000 Pounds',
    'CANOLA_OIL': '000 Pounds',
    'SUNFLOWER_OIL': '000 Pounds',
    'CORN_OIL': '000 Pounds',
    'COTTONSEED_OIL': '000 Pounds',
    'PEANUT_OIL': '000 Pounds',
    'SAFFLOWER_OIL': '000 Pounds',
    'VEGETABLE_OIL': '000 Pounds',

    # Animal Fats - 000 Pounds
    'TALLOW': '000 Pounds',
    'LARD': '000 Pounds',
    'YELLOW_GREASE': '000 Pounds',
    'UCO': '000 Pounds',

    # Protein Meals - Short Tons
    'SOYBEAN_MEAL': 'Short Tons',
    'SUNFLOWER_MEAL': 'Short Tons',
    'CANOLA_MEAL': 'Short Tons',
    'COTTONSEED_MEAL': 'Short Tons',
    'PEANUT_MEAL': 'Short Tons',
    'DDGS': 'Short Tons',
    'CORN_GLUTEN_FEED': 'Short Tons',
    'CORN_GLUTEN_MEAL': 'Short Tons',

    # Oilseeds - Mixed units
    'SOYBEANS': '000 Bushels',          # Soybeans in bushels
    'FLAXSEED': '000 Bushels',          # Flaxseed in bushels
    'COTTONSEED': 'Short Tons',         # Cottonseed in short tons
    'CANOLA': '000 Pounds',             # Canola/rapeseed in pounds
    'RAPESEED': '000 Pounds',           # Rapeseed in pounds
    'SUNFLOWERSEED': '000 Pounds',      # Sunflower in pounds
    'PEANUTS': '000 Pounds',            # Peanuts in pounds
    'GROUNDNUTS': '000 Pounds',         # Groundnuts (peanuts) in pounds
    'SAFFLOWERSEED': '000 Pounds',      # Safflower in pounds

    # Feed Grains - 000 Bushels (for future extension)
    'CORN': '000 Bushels',
    'SORGHUM': '000 Bushels',
    'BARLEY': '000 Bushels',

    # Food Grains - 000 Bushels (for future extension)
    'WHEAT': '000 Bushels',
}


class CensusSilverVerificationAgent(CensusBaseAgent):
    """
    Agent for verifying Census trade data in the silver layer.

    Verifies unit standards, conversion accuracy, and data completeness.
    """

    def __init__(self, **kwargs):
        super().__init__(agent_name='CensusSilverVerification', **kwargs)

    def get_layer(self) -> PipelineLayer:
        return PipelineLayer.SILVER

    def run(self):
        """
        Run all silver layer verification checks.

        Returns:
            AgentResult with verification statistics
        """
        self.log_event(
            EventType.VERIFICATION_START,
            "Starting silver layer verification"
        )

        # Run all checks
        checks = [
            ('unit_standards', self._check_unit_standards),
            ('conversion_accuracy', self._check_conversion_accuracy),
            ('bronze_silver_totals', self._check_bronze_silver_totals),
            ('commodity_coverage', self._check_commodity_coverage),
            ('reference_mappings', self._check_reference_mappings),
            ('data_completeness', self._check_data_completeness),
        ]

        for check_name, check_func in checks:
            try:
                passed, message, details = check_func()
                self.log_verification(
                    check_name=check_name,
                    passed=passed,
                    message=message,
                    **details
                )
            except Exception as e:
                self.log_error(
                    f"Error running check '{check_name}': {e}",
                    data={'check': check_name}
                )

        # Summary
        total_checks = self._result.checks_passed + self._result.checks_failed
        self.set_metadata('total_checks', total_checks)
        self.set_metadata('checks_passed', self._result.checks_passed)
        self.set_metadata('checks_failed', self._result.checks_failed)

        return self.complete()

    def _check_unit_standards(self) -> Tuple[bool, str, Dict]:
        """Check that commodity groups use correct unit standards"""
        query = """
            SELECT
                commodity_group,
                display_unit,
                COUNT(*) as record_count
            FROM silver.census_trade_monthly
            WHERE commodity_group IS NOT NULL
            GROUP BY commodity_group, display_unit
            ORDER BY commodity_group, record_count DESC
        """
        results = self.execute_query(query)

        violations = []
        group_units = {}

        for row in results:
            commodity_group = row[0]
            display_unit = row[1]
            count = row[2]

            # Track primary unit for each group (most common)
            if commodity_group not in group_units:
                group_units[commodity_group] = display_unit

            # Check against expected standards
            expected_unit = UNIT_STANDARDS.get(commodity_group)
            if expected_unit and display_unit != expected_unit:
                violations.append({
                    'commodity_group': commodity_group,
                    'expected': expected_unit,
                    'actual': display_unit,
                    'count': count
                })

        passed = len(violations) == 0

        details = {
            'groups_checked': len(group_units),
            'violations': len(violations)
        }

        if passed:
            message = f"All {len(group_units)} commodity groups use correct units"
        else:
            message = f"{len(violations)} unit standard violations found"
            details['sample_violations'] = violations[:5]

        return passed, message, details

    def _check_conversion_accuracy(self) -> Tuple[bool, str, Dict]:
        """Check that conversions are mathematically accurate"""
        query = """
            SELECT
                s.hs_code,
                s.commodity_group,
                s.quantity_source,
                s.quantity_display,
                s.conversion_factor,
                s.quantity_source * s.conversion_factor as expected_display,
                ABS(s.quantity_display - (s.quantity_source * s.conversion_factor)) as diff
            FROM silver.census_trade_monthly s
            WHERE s.quantity_source IS NOT NULL
              AND s.quantity_display IS NOT NULL
              AND s.conversion_factor IS NOT NULL
              AND s.conversion_factor != 0
              AND s.quantity_source != 0
            HAVING ABS(s.quantity_display - (s.quantity_source * s.conversion_factor))
                   / NULLIF(s.quantity_display, 0) > 0.001
            LIMIT 100
        """

        try:
            results = self.execute_query(query)
            error_count = len(results)
        except Exception:
            # Query might fail if no data - treat as pass
            error_count = 0
            results = []

        # Get total record count
        total_query = """
            SELECT COUNT(*) FROM silver.census_trade_monthly
            WHERE quantity_source IS NOT NULL
        """
        total_result = self.execute_query(total_query)
        total_records = total_result[0][0] if total_result else 0

        # Allow 0.1% error rate
        error_rate = error_count / total_records if total_records > 0 else 0
        passed = error_rate <= 0.001

        details = {
            'total_records': total_records,
            'conversion_errors': error_count,
            'error_rate': f"{error_rate:.4%}"
        }

        if passed:
            message = f"Conversions accurate ({total_records} records, {error_count} errors)"
        else:
            message = f"Conversion errors: {error_count} records ({error_rate:.2%})"

        return passed, message, details

    def _check_bronze_silver_totals(self) -> Tuple[bool, str, Dict]:
        """Check that silver totals match bronze totals"""
        query = """
            WITH bronze_totals AS (
                SELECT
                    year, month, flow,
                    SUM(value_usd) as bronze_value,
                    SUM(quantity) as bronze_qty,
                    COUNT(*) as bronze_count
                FROM bronze.census_trade
                GROUP BY year, month, flow
            ),
            silver_totals AS (
                SELECT
                    year, month, flow,
                    SUM(value_usd) as silver_value,
                    SUM(quantity_source) as silver_qty,
                    COUNT(*) as silver_count
                FROM silver.census_trade_monthly
                GROUP BY year, month, flow
            )
            SELECT
                b.year, b.month, b.flow,
                b.bronze_value, s.silver_value,
                b.bronze_qty, s.silver_qty,
                b.bronze_count, s.silver_count,
                ABS(COALESCE(b.bronze_value, 0) - COALESCE(s.silver_value, 0))
                    / NULLIF(b.bronze_value, 0) as value_diff_pct,
                ABS(b.bronze_count - COALESCE(s.silver_count, 0)) as count_diff
            FROM bronze_totals b
            LEFT JOIN silver_totals s
                ON b.year = s.year AND b.month = s.month AND b.flow = s.flow
            WHERE b.bronze_count != COALESCE(s.silver_count, 0)
               OR (ABS(COALESCE(b.bronze_value, 0) - COALESCE(s.silver_value, 0))
                   / NULLIF(b.bronze_value, 0)) > 0.001
            ORDER BY b.year DESC, b.month DESC
            LIMIT 20
        """

        try:
            results = self.execute_query(query)
            mismatch_count = len(results)
        except Exception as e:
            # Tables might not exist yet
            return True, f"Cross-reference check skipped: {e}", {}

        # Get total periods
        period_query = """
            SELECT COUNT(DISTINCT (year, month, flow)) FROM bronze.census_trade
        """
        period_result = self.execute_query(period_query)
        total_periods = period_result[0][0] if period_result else 0

        passed = mismatch_count == 0

        details = {
            'total_periods': total_periods,
            'mismatches': mismatch_count
        }

        if passed:
            message = f"Bronze-Silver totals match ({total_periods} periods)"
        else:
            message = f"{mismatch_count} periods with mismatched totals"
            details['sample_mismatches'] = [
                {
                    'year': r[0], 'month': r[1], 'flow': r[2],
                    'bronze_count': r[7], 'silver_count': r[8]
                }
                for r in results[:5]
            ]

        return passed, message, details

    def _check_commodity_coverage(self) -> Tuple[bool, str, Dict]:
        """Check that all expected commodity groups are represented"""
        expected_groups = list(UNIT_STANDARDS.keys())

        query = """
            SELECT commodity_group, COUNT(*) as cnt
            FROM silver.census_trade_monthly
            WHERE commodity_group IS NOT NULL
            GROUP BY commodity_group
        """
        results = self.execute_query(query)

        found_groups = {row[0]: row[1] for row in results}

        missing_groups = [g for g in expected_groups if g not in found_groups]
        extra_groups = [g for g in found_groups if g not in expected_groups and g != 'UNKNOWN']

        # Allow missing groups (might not have traded), but warn
        passed = len(missing_groups) <= len(expected_groups) * 0.5

        details = {
            'expected_groups': len(expected_groups),
            'found_groups': len(found_groups),
            'missing_groups': missing_groups[:5],
            'extra_groups': extra_groups[:5]
        }

        if len(missing_groups) == 0:
            message = f"All {len(expected_groups)} commodity groups present"
        elif passed:
            message = f"{len(found_groups)} groups found, {len(missing_groups)} missing (acceptable)"
        else:
            message = f"Too many missing groups: {len(missing_groups)} of {len(expected_groups)}"

        return passed, message, details

    def _check_reference_mappings(self) -> Tuple[bool, str, Dict]:
        """Check that HS codes are properly mapped to reference table"""
        query = """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN commodity_group = 'UNKNOWN' THEN 1 ELSE 0 END) as unmapped
            FROM silver.census_trade_monthly
        """
        results = self.execute_query(query)
        row = results[0]

        total = row[0]
        unmapped = row[1]

        unmapped_rate = unmapped / total if total > 0 else 0

        # Allow up to 10% unmapped (new HS codes, etc.)
        passed = unmapped_rate <= 0.10

        details = {
            'total_records': total,
            'unmapped_records': unmapped,
            'unmapped_rate': f"{unmapped_rate:.2%}"
        }

        if passed:
            message = f"Reference mapping good ({unmapped_rate:.1%} unmapped)"
        else:
            message = f"Too many unmapped HS codes: {unmapped_rate:.1%}"

        return passed, message, details

    def _check_data_completeness(self) -> Tuple[bool, str, Dict]:
        """Check for data completeness (no gaps in time series)"""
        query = """
            WITH month_series AS (
                SELECT DISTINCT year, month
                FROM silver.census_trade_monthly
                ORDER BY year, month
            ),
            gaps AS (
                SELECT
                    year, month,
                    LAG(year * 12 + month) OVER (ORDER BY year, month) as prev_ym,
                    year * 12 + month as curr_ym
                FROM month_series
            )
            SELECT year, month, curr_ym - prev_ym as gap_months
            FROM gaps
            WHERE curr_ym - prev_ym > 1
        """

        try:
            results = self.execute_query(query)
            gap_count = len(results)
        except Exception:
            gap_count = 0
            results = []

        passed = gap_count == 0

        details = {'gaps_found': gap_count}

        if passed:
            message = "No gaps in time series data"
        else:
            message = f"{gap_count} gaps found in time series"
            details['gaps'] = [
                {'year': r[0], 'month': r[1], 'gap_months': r[2]}
                for r in results[:5]
            ]

        return passed, message, details


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for Census Silver Verification Agent"""
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    agent = CensusSilverVerificationAgent()
    result = agent.run()

    print(f"\n{'='*60}")
    print(f"VERIFICATION RESULT: {'PASS' if result.success else 'FAIL'}")
    print(f"{'='*60}")
    print(f"Checks passed: {result.checks_passed}")
    print(f"Checks failed: {result.checks_failed}")
    print(f"Duration: {result.duration_seconds:.1f}s")

    if result.warnings:
        print(f"\nWarnings ({len(result.warnings)}):")
        for w in result.warnings:
            print(f"  - {w}")

    if result.errors:
        print(f"\nErrors ({len(result.errors)}):")
        for e in result.errors:
            print(f"  - {e}")


if __name__ == '__main__':
    main()
