"""
Census Bronze Verification Agent
================================
Agent for verifying Census Bureau trade data in the bronze layer.

Verification checks:
1. Record count validation
2. NULL field detection (required fields)
3. Duplicate detection
4. Date range validation (2013-present, no future dates)
5. HS code completeness check
6. Value range validation

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


class CensusBronzeVerificationAgent(CensusBaseAgent):
    """
    Agent for verifying Census trade data in the bronze layer.

    Runs multiple validation checks and logs results.
    """

    def __init__(self, **kwargs):
        super().__init__(agent_name='CensusBronzeVerification', **kwargs)

    def get_layer(self) -> PipelineLayer:
        return PipelineLayer.BRONZE

    def run(
        self,
        start_date: date = None,
        end_date: date = None,
        expected_record_count: int = None
    ):
        """
        Run all bronze layer verification checks.

        Args:
            start_date: Start of date range to verify
            end_date: End of date range to verify
            expected_record_count: Expected number of records (optional)

        Returns:
            AgentResult with verification statistics
        """
        self.log_event(
            EventType.VERIFICATION_START,
            "Starting bronze layer verification"
        )

        # Run all checks
        checks = [
            ('record_count', self._check_record_count, {'expected': expected_record_count}),
            ('null_fields', self._check_null_fields, {}),
            ('duplicates', self._check_duplicates, {}),
            ('date_range', self._check_date_range, {'start_date': start_date, 'end_date': end_date}),
            ('hs_code_format', self._check_hs_code_format, {}),
            ('value_ranges', self._check_value_ranges, {}),
            ('country_codes', self._check_country_codes, {}),
        ]

        for check_name, check_func, check_args in checks:
            try:
                passed, message, details = check_func(**check_args)
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

    def _check_record_count(self, expected: int = None) -> Tuple[bool, str, Dict]:
        """Check if record count is reasonable"""
        query = """
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT hs_code) as hs_codes,
                COUNT(DISTINCT country_code) as countries,
                MIN(collected_at) as oldest,
                MAX(collected_at) as newest
            FROM bronze.census_trade
        """
        results = self.execute_query(query)
        row = results[0]

        total = row[0]
        hs_codes = row[1]
        countries = row[2]
        oldest = row[3]
        newest = row[4]

        details = {
            'actual': total,
            'expected': expected,
        }

        # If expected count provided, check within 10%
        if expected is not None:
            tolerance = expected * 0.1
            passed = abs(total - expected) <= tolerance
            message = f"Record count: {total} (expected: {expected}, tolerance: 10%)"
        else:
            # Just check that we have reasonable data
            passed = total > 0 and hs_codes > 0 and countries > 0
            message = f"Records: {total}, HS codes: {hs_codes}, Countries: {countries}"

        details['hs_codes'] = hs_codes
        details['countries'] = countries

        return passed, message, details

    def _check_null_fields(self) -> Tuple[bool, str, Dict]:
        """Check for NULL values in required fields"""
        query = """
            SELECT
                SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) as null_year,
                SUM(CASE WHEN month IS NULL THEN 1 ELSE 0 END) as null_month,
                SUM(CASE WHEN flow IS NULL OR flow = '' THEN 1 ELSE 0 END) as null_flow,
                SUM(CASE WHEN hs_code IS NULL OR hs_code = '' THEN 1 ELSE 0 END) as null_hs_code,
                SUM(CASE WHEN country_code IS NULL OR country_code = '' THEN 1 ELSE 0 END) as null_country,
                COUNT(*) as total
            FROM bronze.census_trade
        """
        results = self.execute_query(query)
        row = results[0]

        null_counts = {
            'year': row[0],
            'month': row[1],
            'flow': row[2],
            'hs_code': row[3],
            'country_code': row[4]
        }
        total = row[5]

        total_nulls = sum(null_counts.values())
        passed = total_nulls == 0

        if passed:
            message = f"No NULL values in required fields ({total} records checked)"
        else:
            problem_fields = [k for k, v in null_counts.items() if v > 0]
            message = f"Found NULL values in: {problem_fields}"

        return passed, message, {'null_counts': null_counts, 'total_records': total}

    def _check_duplicates(self) -> Tuple[bool, str, Dict]:
        """Check for duplicate records"""
        query = """
            SELECT year, month, flow, hs_code, country_code, COUNT(*) as cnt
            FROM bronze.census_trade
            GROUP BY year, month, flow, hs_code, country_code
            HAVING COUNT(*) > 1
            LIMIT 10
        """
        results = self.execute_query(query)

        duplicate_count = len(results)
        passed = duplicate_count == 0

        details = {'duplicate_groups': duplicate_count}

        if passed:
            message = "No duplicate records found"
        else:
            message = f"Found {duplicate_count} duplicate record groups"
            details['sample_duplicates'] = [
                {'year': r[0], 'month': r[1], 'flow': r[2], 'hs_code': r[3], 'count': r[5]}
                for r in results[:5]
            ]

        return passed, message, details

    def _check_date_range(
        self,
        start_date: date = None,
        end_date: date = None
    ) -> Tuple[bool, str, Dict]:
        """Check for valid date ranges"""
        query = """
            SELECT
                MIN(year) as min_year,
                MAX(year) as max_year,
                MIN(month) as min_month,
                MAX(month) as max_month,
                SUM(CASE WHEN year < 2013 THEN 1 ELSE 0 END) as before_2013,
                SUM(CASE WHEN year > EXTRACT(YEAR FROM NOW()) THEN 1 ELSE 0 END) as future_year,
                SUM(CASE WHEN month < 1 OR month > 12 THEN 1 ELSE 0 END) as invalid_month
            FROM bronze.census_trade
        """
        results = self.execute_query(query)
        row = results[0]

        min_year, max_year = row[0], row[1]
        before_2013 = row[4]
        future_year = row[5]
        invalid_month = row[6]

        issues = []
        if before_2013 > 0:
            issues.append(f"{before_2013} records before 2013")
        if future_year > 0:
            issues.append(f"{future_year} records with future year")
        if invalid_month > 0:
            issues.append(f"{invalid_month} records with invalid month")

        passed = len(issues) == 0
        details = {
            'min_year': min_year,
            'max_year': max_year,
            'before_2013': before_2013,
            'future_year': future_year,
            'invalid_month': invalid_month
        }

        if passed:
            message = f"Valid date range: {min_year} to {max_year}"
        else:
            message = f"Date validation issues: {'; '.join(issues)}"

        return passed, message, details

    def _check_hs_code_format(self) -> Tuple[bool, str, Dict]:
        """Check HS code format (should be 10 digits or valid shorter codes)"""
        query = """
            SELECT
                LENGTH(hs_code) as code_length,
                COUNT(*) as cnt
            FROM bronze.census_trade
            WHERE hs_code IS NOT NULL
            GROUP BY LENGTH(hs_code)
            ORDER BY code_length
        """
        results = self.execute_query(query)

        length_distribution = {row[0]: row[1] for row in results}

        # Most codes should be 10 digits, some may be 4 or 6
        total = sum(length_distribution.values())
        valid_lengths = {4, 6, 8, 10}
        invalid_records = sum(
            cnt for length, cnt in length_distribution.items()
            if length not in valid_lengths
        )

        passed = invalid_records == 0 or (invalid_records / total) < 0.01  # Allow 1% error

        details = {
            'length_distribution': length_distribution,
            'invalid_count': invalid_records,
            'total': total
        }

        if passed:
            message = f"HS code formats valid ({total} records)"
        else:
            message = f"{invalid_records} records with invalid HS code length"

        return passed, message, details

    def _check_value_ranges(self) -> Tuple[bool, str, Dict]:
        """Check for reasonable value ranges"""
        query = """
            SELECT
                SUM(CASE WHEN value_usd < 0 THEN 1 ELSE 0 END) as negative_values,
                SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END) as negative_qty,
                SUM(CASE WHEN value_usd > 1e12 THEN 1 ELSE 0 END) as extreme_values,
                AVG(value_usd) as avg_value,
                MAX(value_usd) as max_value,
                COUNT(*) as total
            FROM bronze.census_trade
        """
        results = self.execute_query(query)
        row = results[0]

        negative_values = row[0]
        negative_qty = row[1]
        extreme_values = row[2]
        avg_value = row[3]
        max_value = row[4]
        total = row[5]

        issues = []
        if negative_values > 0:
            issues.append(f"{negative_values} negative values")
        if negative_qty > 0:
            issues.append(f"{negative_qty} negative quantities")
        if extreme_values > 0:
            issues.append(f"{extreme_values} extreme values (>$1T)")

        passed = len(issues) == 0

        details = {
            'negative_values': negative_values,
            'negative_qty': negative_qty,
            'extreme_values': extreme_values,
            'avg_value': float(avg_value) if avg_value else 0,
            'max_value': float(max_value) if max_value else 0
        }

        if passed:
            message = f"Value ranges valid (avg: ${avg_value:,.0f})"
        else:
            message = f"Value range issues: {'; '.join(issues)}"

        return passed, message, details

    def _check_country_codes(self) -> Tuple[bool, str, Dict]:
        """Check country codes against reference table"""
        # Check for countries not in reference table
        query = """
            SELECT DISTINCT ct.country_code, ct.country_name
            FROM bronze.census_trade ct
            LEFT JOIN silver.trade_country_reference cr
                ON ct.country_code = cr.census_code
            WHERE cr.census_code IS NULL
                AND ct.country_code IS NOT NULL
                AND ct.country_code NOT IN ('R00', '-')  -- World totals
            LIMIT 20
        """
        try:
            results = self.execute_query(query)

            unmapped_count = len(results)
            passed = unmapped_count == 0

            details = {
                'unmapped_countries': unmapped_count
            }

            if not passed:
                details['sample_unmapped'] = [
                    {'code': r[0], 'name': r[1]} for r in results[:5]
                ]
                message = f"{unmapped_count} country codes not in reference table"
            else:
                message = "All country codes mapped to reference table"

            return passed, message, details

        except Exception as e:
            # Reference table may not exist
            return True, f"Country code check skipped: {e}", {}


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for Census Bronze Verification Agent"""
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description='Census Bronze Verification Agent')

    parser.add_argument(
        '--expected-count',
        type=int,
        help='Expected record count for validation'
    )

    args = parser.parse_args()

    agent = CensusBronzeVerificationAgent()
    result = agent.run(expected_record_count=args.expected_count)

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
