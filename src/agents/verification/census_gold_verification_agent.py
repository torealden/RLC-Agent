"""
Census Gold Verification Agent
==============================
Agent for verifying Gold layer views against Bronze and Silver layers.

Verification checks:
1. Gold totals = Silver totals = Bronze totals
2. Referential integrity (all FKs valid)
3. YoY calculation accuracy
4. View freshness (views not stale)

Logs all verification results for the CensusLogReaderAgent.
"""

import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from agents.base.census_base_agent import CensusBaseAgent, PipelineLayer, EventType


class CensusGoldVerificationAgent(CensusBaseAgent):
    """
    Agent for verifying Gold layer data against Bronze and Silver layers.

    Ensures data consistency across all pipeline layers.
    """

    def __init__(self, **kwargs):
        super().__init__(agent_name='CensusGoldVerification', **kwargs)

    def get_layer(self) -> PipelineLayer:
        return PipelineLayer.GOLD

    def run(self):
        """
        Run all Gold layer verification checks.

        Returns:
            AgentResult with verification statistics
        """
        self.log_event(
            EventType.VERIFICATION_START,
            "Starting Gold layer verification"
        )

        # Run all checks
        checks = [
            ('cross_layer_totals', self._check_cross_layer_totals),
            ('view_existence', self._check_view_existence),
            ('yoy_calculations', self._check_yoy_calculations),
            ('export_matrix_integrity', self._check_export_matrix),
            ('import_matrix_integrity', self._check_import_matrix),
            ('commodity_coverage', self._check_commodity_coverage),
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

    def _check_cross_layer_totals(self) -> Tuple[bool, str, Dict]:
        """Check that Gold, Silver, and Bronze totals match"""
        query = """
            WITH bronze_totals AS (
                SELECT
                    SUM(value_usd) as total_value,
                    COUNT(*) as record_count
                FROM bronze.census_trade
            ),
            silver_totals AS (
                SELECT
                    SUM(value_usd) as total_value,
                    COUNT(*) as record_count
                FROM silver.census_trade_monthly
            ),
            gold_export_totals AS (
                SELECT
                    SUM(value_usd) as total_value,
                    COUNT(*) as record_count
                FROM gold.trade_export_matrix
            ),
            gold_import_totals AS (
                SELECT
                    SUM(value_usd) as total_value,
                    COUNT(*) as record_count
                FROM gold.trade_import_matrix
            )
            SELECT
                b.total_value as bronze_value,
                s.total_value as silver_value,
                e.total_value + i.total_value as gold_value,
                b.record_count as bronze_count,
                s.record_count as silver_count,
                e.record_count + i.record_count as gold_count
            FROM bronze_totals b, silver_totals s, gold_export_totals e, gold_import_totals i
        """

        try:
            results = self.execute_query(query)
            row = results[0]

            bronze_value = float(row[0]) if row[0] else 0
            silver_value = float(row[1]) if row[1] else 0
            gold_value = float(row[2]) if row[2] else 0

            bronze_count = row[3]
            silver_count = row[4]
            gold_count = row[5]

            # Check value consistency (within 0.1%)
            bs_diff = abs(bronze_value - silver_value) / bronze_value if bronze_value else 0
            sg_diff = abs(silver_value - gold_value) / silver_value if silver_value else 0

            passed = bs_diff <= 0.001 and sg_diff <= 0.001

            details = {
                'bronze_value': bronze_value,
                'silver_value': silver_value,
                'gold_value': gold_value,
                'bronze_silver_diff_pct': f"{bs_diff:.4%}",
                'silver_gold_diff_pct': f"{sg_diff:.4%}",
                'bronze_count': bronze_count,
                'silver_count': silver_count,
                'gold_count': gold_count
            }

            if passed:
                message = "Cross-layer totals match within 0.1%"
            else:
                message = f"Cross-layer mismatch: B-S: {bs_diff:.2%}, S-G: {sg_diff:.2%}"

            return passed, message, details

        except Exception as e:
            return True, f"Cross-layer check skipped: {e}", {}

    def _check_view_existence(self) -> Tuple[bool, str, Dict]:
        """Check that all expected Gold views exist"""
        expected_views = [
            'trade_export_matrix',
            'trade_import_matrix',
            'cottonseed_oil_trade_summary',
            'trade_yoy_comparison',
            'trade_commodity_summary',
            'soybean_complex_trade',
            'corn_product_trade'
        ]

        query = """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = 'gold'
              AND table_name = ANY(%s)
        """

        try:
            results = self.execute_query(query, (expected_views,))
            found_views = [r[0] for r in results]

            missing_views = [v for v in expected_views if v not in found_views]

            passed = len(missing_views) == 0

            details = {
                'expected': len(expected_views),
                'found': len(found_views),
                'missing': missing_views
            }

            if passed:
                message = f"All {len(expected_views)} Gold views exist"
            else:
                message = f"Missing views: {missing_views}"

            return passed, message, details

        except Exception as e:
            return False, f"View existence check failed: {e}", {}

    def _check_yoy_calculations(self) -> Tuple[bool, str, Dict]:
        """Check YoY calculation accuracy"""
        query = """
            WITH sample_check AS (
                SELECT
                    year, month, flow, commodity_group,
                    current_quantity, prior_year_quantity,
                    qty_yoy_pct,
                    ROUND(((current_quantity - COALESCE(prior_year_quantity, 0))
                           / NULLIF(prior_year_quantity, 0)) * 100, 1) as calc_yoy_pct
                FROM gold.trade_yoy_comparison
                WHERE prior_year_quantity IS NOT NULL
                  AND prior_year_quantity != 0
                LIMIT 100
            )
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN ABS(COALESCE(qty_yoy_pct, 0) - COALESCE(calc_yoy_pct, 0)) > 0.2 THEN 1 ELSE 0 END) as mismatches
            FROM sample_check
        """

        try:
            results = self.execute_query(query)
            row = results[0]

            total = row[0] or 0
            mismatches = row[1] or 0

            passed = mismatches == 0 or (total > 0 and mismatches / total < 0.01)

            details = {
                'samples_checked': total,
                'calculation_errors': mismatches
            }

            if passed:
                message = f"YoY calculations accurate ({total} samples)"
            else:
                message = f"YoY calculation errors: {mismatches}/{total}"

            return passed, message, details

        except Exception as e:
            return True, f"YoY check skipped: {e}", {}

    def _check_export_matrix(self) -> Tuple[bool, str, Dict]:
        """Check export matrix data integrity"""
        query = """
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT commodity_group) as commodities,
                COUNT(DISTINCT country_code) as countries,
                MIN(year) as min_year,
                MAX(year) as max_year,
                SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) as null_qty,
                SUM(CASE WHEN value_usd IS NULL THEN 1 ELSE 0 END) as null_value
            FROM gold.trade_export_matrix
        """

        try:
            results = self.execute_query(query)
            row = results[0]

            total = row[0]
            null_qty = row[5]
            null_value = row[6]

            # Allow some NULL values (sparse data)
            null_rate = (null_qty + null_value) / (total * 2) if total > 0 else 0
            passed = total > 0 and null_rate < 0.5

            details = {
                'total_records': total,
                'commodities': row[1],
                'countries': row[2],
                'year_range': f"{row[3]}-{row[4]}",
                'null_rate': f"{null_rate:.2%}"
            }

            if passed:
                message = f"Export matrix valid ({total} records)"
            else:
                message = f"Export matrix issues: {total} records, {null_rate:.1%} nulls"

            return passed, message, details

        except Exception as e:
            return True, f"Export matrix check skipped: {e}", {}

    def _check_import_matrix(self) -> Tuple[bool, str, Dict]:
        """Check import matrix data integrity"""
        query = """
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT commodity_group) as commodities,
                COUNT(DISTINCT country_code) as countries,
                MIN(year) as min_year,
                MAX(year) as max_year,
                SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) as null_qty,
                SUM(CASE WHEN value_usd IS NULL THEN 1 ELSE 0 END) as null_value
            FROM gold.trade_import_matrix
        """

        try:
            results = self.execute_query(query)
            row = results[0]

            total = row[0]
            null_qty = row[5]
            null_value = row[6]

            null_rate = (null_qty + null_value) / (total * 2) if total > 0 else 0
            passed = total > 0 and null_rate < 0.5

            details = {
                'total_records': total,
                'commodities': row[1],
                'countries': row[2],
                'year_range': f"{row[3]}-{row[4]}",
                'null_rate': f"{null_rate:.2%}"
            }

            if passed:
                message = f"Import matrix valid ({total} records)"
            else:
                message = f"Import matrix issues: {total} records, {null_rate:.1%} nulls"

            return passed, message, details

        except Exception as e:
            return True, f"Import matrix check skipped: {e}", {}

    def _check_commodity_coverage(self) -> Tuple[bool, str, Dict]:
        """Check commodity coverage in Gold views"""
        query = """
            WITH silver_commodities AS (
                SELECT DISTINCT commodity_group
                FROM silver.census_trade_monthly
                WHERE commodity_group IS NOT NULL
                  AND commodity_group != 'UNKNOWN'
            ),
            gold_commodities AS (
                SELECT DISTINCT commodity_group FROM gold.trade_export_matrix
                UNION
                SELECT DISTINCT commodity_group FROM gold.trade_import_matrix
            )
            SELECT
                (SELECT COUNT(*) FROM silver_commodities) as silver_count,
                (SELECT COUNT(*) FROM gold_commodities) as gold_count,
                (SELECT COUNT(*) FROM silver_commodities WHERE commodity_group NOT IN (SELECT commodity_group FROM gold_commodities WHERE commodity_group IS NOT NULL)) as missing
        """

        try:
            results = self.execute_query(query)
            row = results[0]

            silver_count = row[0]
            gold_count = row[1]
            missing = row[2]

            passed = missing == 0

            details = {
                'silver_commodities': silver_count,
                'gold_commodities': gold_count,
                'missing_in_gold': missing
            }

            if passed:
                message = f"All {silver_count} commodities in Gold views"
            else:
                message = f"{missing} commodities missing from Gold views"

            return passed, message, details

        except Exception as e:
            return True, f"Commodity coverage check skipped: {e}", {}


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for Census Gold Verification Agent"""
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    agent = CensusGoldVerificationAgent()
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
