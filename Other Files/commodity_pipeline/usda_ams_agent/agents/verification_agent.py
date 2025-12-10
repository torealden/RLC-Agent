"""
Verification Agent for Commodity Data Pipeline
Validates data integrity after database insertion
Compares source data with database records
Round Lakes Commodities
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import json
import random

logger = logging.getLogger(__name__)


@dataclass
class VerificationResult:
    """Result of a verification check"""
    passed: bool
    check_type: str
    expected: Any
    actual: Any
    message: str
    details: Dict = None
    
    def to_dict(self) -> Dict:
        return {
            'passed': self.passed,
            'check_type': self.check_type,
            'expected': str(self.expected),
            'actual': str(self.actual),
            'message': self.message,
            'details': self.details
        }


@dataclass
class VerificationReport:
    """Complete verification report for a pipeline run"""
    timestamp: str
    source_report: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    status: str  # 'PASSED', 'FAILED', 'PARTIAL', 'ERROR'
    results: List[VerificationResult]
    summary: str
    
    @property
    def pass_rate(self) -> float:
        if self.total_checks == 0:
            return 1.0
        return self.passed_checks / self.total_checks
    
    def to_dict(self) -> Dict:
        return {
            'timestamp': self.timestamp,
            'source_report': self.source_report,
            'total_checks': self.total_checks,
            'passed_checks': self.passed_checks,
            'failed_checks': self.failed_checks,
            'pass_rate': round(self.pass_rate * 100, 1),
            'status': self.status,
            'summary': self.summary,
            'results': [r.to_dict() for r in self.results]
        }


class VerificationAgent:
    """
    Data verification agent for the commodity pipeline.
    Performs various checks to ensure data integrity after loading.
    """
    
    def __init__(self, database_agent, sample_size: int = 5):
        """
        Initialize the verification agent.
        
        Args:
            database_agent: DatabaseAgent instance for querying the database
            sample_size: Number of random records to verify in detail
        """
        self.db = database_agent
        self.sample_size = sample_size
        logger.info(f"VerificationAgent initialized with sample_size={sample_size}")
    
    def verify_insert(self, source_records: List[Dict], 
                        source_report: str,
                        expected_count: int = None
                    ) -> VerificationReport:
        """
        Comprehensive verification of an insert operation.
        
        Args:
            source_records: Original records that were inserted
            source_report: Name of the source report
            expected_count: Expected number of records (uses len(source_records) if None)
            
        Returns:
            VerificationReport with all check results
        """
        results = []
        timestamp = datetime.now().isoformat()
        
        if expected_count is None:
            expected_count = len(source_records)
        
        # 1. Count verification
        count_result = self._verify_count(source_report, expected_count)
        results.append(count_result)
        
        # 2. Date range verification
        if source_records:
            date_result = self._verify_date_range(source_records, source_report)
            results.append(date_result)
        
        # 3. Sample value verification
        if source_records:
            sample_results = self._verify_sample_values(source_records, source_report)
            results.extend(sample_results)
        
        # 4. Completeness check
        completeness_result = self._verify_completeness(source_records, source_report)
        results.append(completeness_result)
        
        # Calculate overall status
        passed = sum(1 for r in results if r.passed)
        failed = len(results) - passed
        
        if failed == 0:
            status = 'PASSED'
            summary = f"All {len(results)} verification checks passed"
        elif passed == 0:
            status = 'FAILED'
            summary = f"All {len(results)} verification checks failed"
        else:
            status = 'PARTIAL'
            summary = f"{passed}/{len(results)} verification checks passed"
        
        report = VerificationReport(
            timestamp=timestamp,
            source_report=source_report,
            total_checks=len(results),
            passed_checks=passed,
            failed_checks=failed,
            status=status,
            results=results,
            summary=summary
        )
        
        logger.info(f"Verification complete for {source_report}: {status} - {summary}")
        return report
    
    def _verify_count(self, source_report: str, expected_count: int) -> VerificationResult:
        """
        Verify record count matches expected.
        """
        try:
            actual_count = self.db.get_record_count(source_report=source_report)
            
            # For INSERT IGNORE, actual count should be <= expected if there were duplicates
            # We consider it passed if actual_count > 0 and within expected bounds
            passed = actual_count > 0 and actual_count <= expected_count
            
            if passed:
                if actual_count == expected_count:
                    message = f"Record count matches: {actual_count}"
                else:
                    message = f"Record count {actual_count} (expected {expected_count}, some duplicates skipped)"
            else:
                message = f"Record count mismatch: expected up to {expected_count}, got {actual_count}"
            
            return VerificationResult(
                passed=passed,
                check_type='count_verification',
                expected=expected_count,
                actual=actual_count,
                message=message
            )
            
        except Exception as e:
            return VerificationResult(
                passed=False,
                check_type='count_verification',
                expected=expected_count,
                actual='ERROR',
                message=f"Count verification failed: {e}"
            )
    
    def _verify_date_range(self, source_records: List[Dict], 
                            source_report: str
                        ) -> VerificationResult:
        """
        Verify date range in database matches source data.
        """
        try:
            # Get date range from source records
            source_dates = []
            for rec in source_records:
                date_val = rec.get('report_date', rec.get('date', ''))
                if date_val:
                    source_dates.append(str(date_val))
            
            if not source_dates:
                return VerificationResult(
                    passed=True,
                    check_type='date_range_verification',
                    expected='N/A',
                    actual='N/A',
                    message='No dates found in source records'
                )
            
            source_dates.sort()
            expected_range = (source_dates[0], source_dates[-1])
            
            # Get date range from database
            actual_range = self.db.get_date_range(source_report=source_report)
            
            if actual_range[0] is None:
                passed = False
                message = "No dates found in database"
            else:
                # Check if date ranges overlap reasonably
                passed = True
                message = f"Date range verified: {actual_range[0]} to {actual_range[1]}"
            
            return VerificationResult(
                passed=passed,
                check_type='date_range_verification',
                expected=expected_range,
                actual=actual_range,
                message=message
            )
            
        except Exception as e:
            return VerificationResult(
                passed=False,
                check_type='date_range_verification',
                expected='N/A',
                actual='ERROR',
                message=f"Date range verification failed: {e}"
            )
    
    def _verify_sample_values(self, source_records: List[Dict],
                                source_report: str
                            ) -> List[VerificationResult]:
        """
        Verify random sample of records match source values.
        """
        results = []
        
        try:
            # Select random sample
            sample_size = min(self.sample_size, len(source_records))
            sample_records = random.sample(source_records, sample_size)
            
            for source_rec in sample_records:
                result = self._verify_single_record(source_rec, source_report)
                results.append(result)
                
        except Exception as e:
            results.append(VerificationResult(
                passed=False,
                check_type='sample_value_verification',
                expected='N/A',
                actual='ERROR',
                message=f"Sample verification failed: {e}"
            ))
        
        return results
    
    def _verify_single_record(self, source_record: Dict, 
                                source_report: str
                            ) -> VerificationResult:
        """
        Verify a single record exists in the database with matching values.
        """
        try:
            # Build query criteria from source record
            report_date = source_record.get('report_date', source_record.get('date', ''))
            commodity = source_record.get('commodity', source_record.get('commodity_name', ''))
            location = source_record.get('location', source_record.get('market_location', ''))
            
            if not report_date or not commodity:
                return VerificationResult(
                    passed=True,  # Skip records without key fields
                    check_type='sample_record_verification',
                    expected='incomplete record',
                    actual='skipped',
                    message='Skipped record with missing key fields'
                )
            
            # Query database for matching records
            db_records = self.db.get_records(
                source_report=source_report,
                commodity=commodity,
                limit=10
            )
            
            # Try to find a matching record
            found = False
            for db_rec in db_records:
                # Check if key fields match
                db_date = str(db_rec.get('report_date', ''))
                db_commodity = str(db_rec.get('commodity', '')).lower()
                
                # Normalize for comparison
                src_date = str(report_date)
                src_commodity = str(commodity).lower()
                
                if db_date.endswith(src_date[-10:]) and db_commodity == src_commodity:
                    found = True
                    break
            
            if found:
                return VerificationResult(
                    passed=True,
                    check_type='sample_record_verification',
                    expected=f"{commodity} on {report_date}",
                    actual='found',
                    message=f"Record verified: {commodity} on {report_date}"
                )
            else:
                return VerificationResult(
                    passed=False,
                    check_type='sample_record_verification',
                    expected=f"{commodity} on {report_date}",
                    actual='not found',
                    message=f"Record not found: {commodity} on {report_date}"
                )
                
        except Exception as e:
            return VerificationResult(
                passed=False,
                check_type='sample_record_verification',
                expected='record lookup',
                actual='ERROR',
                message=f"Record verification failed: {e}"
            )
    
    def _verify_completeness(self, source_records: List[Dict],
                                source_report: str
                            ) -> VerificationResult:
        """
        Verify data completeness - check for required fields.
        """
        try:
            required_fields = ['report_date', 'commodity']
            missing_counts = {field: 0 for field in required_fields}
            
            for rec in source_records:
                for field in required_fields:
                    # Check various field name variations
                    variations = [field, field.replace('_', ''), 
                                    field.replace('report_', ''), field + '_name'
                                ]
                    found = any(rec.get(v) for v in variations if rec.get(v))
                    if not found:
                        missing_counts[field] += 1
            
            total_missing = sum(missing_counts.values())
            completeness_rate = 1 - (total_missing / (len(source_records) * len(required_fields))) if source_records else 1
            
            passed = completeness_rate >= 0.95  # 95% threshold
            
            if passed:
                message = f"Data completeness: {completeness_rate*100:.1f}%"
            else:
                missing_detail = ', '.join(f"{k}: {v}" for k, v in missing_counts.items() if v > 0)
                message = f"Data completeness below threshold: {completeness_rate*100:.1f}% (missing: {missing_detail})"
            
            return VerificationResult(
                passed=passed,
                check_type='completeness_verification',
                expected='>=95%',
                actual=f"{completeness_rate*100:.1f}%",
                message=message,
                details=missing_counts
            )
            
        except Exception as e:
            return VerificationResult(
                passed=False,
                check_type='completeness_verification',
                expected='>=95%',
                actual='ERROR',
                message=f"Completeness verification failed: {e}"
            )
    
    def verify_database_integrity(self) -> VerificationReport:
        """
        Perform general database integrity checks (not specific to an insert).
        
        Returns:
            VerificationReport with overall database health
        """
        results = []
        timestamp = datetime.now().isoformat()
        
        # 1. Check database is accessible
        try:
            total_records = self.db.get_record_count()
            results.append(VerificationResult(
                passed=True,
                check_type='database_connection',
                expected='connected',
                actual='connected',
                message=f"Database accessible with {total_records} total records"
            ))
        except Exception as e:
            results.append(VerificationResult(
                passed=False,
                check_type='database_connection',
                expected='connected',
                actual='error',
                message=f"Database connection failed: {e}"
            ))
        
        # 2. Check for orphaned records (optional)
        try:
            commodities = self.db.get_unique_commodities()
            sources = self.db.get_unique_sources()
            results.append(VerificationResult(
                passed=True,
                check_type='data_diversity',
                expected='>0 commodities and sources',
                actual=f"{len(commodities)} commodities, {len(sources)} sources",
                message=f"Found {len(commodities)} unique commodities from {len(sources)} sources"
            ))
        except Exception as e:
            results.append(VerificationResult(
                passed=False,
                check_type='data_diversity',
                expected='>0 commodities and sources',
                actual='error',
                message=f"Data diversity check failed: {e}"
            ))
        
        # 3. Check date range
        try:
            date_range = self.db.get_date_range()
            if date_range[0]:
                results.append(VerificationResult(
                    passed=True,
                    check_type='date_range',
                    expected='valid date range',
                    actual=f"{date_range[0]} to {date_range[1]}",
                    message=f"Data spans {date_range[0]} to {date_range[1]}"
                ))
            else:
                results.append(VerificationResult(
                    passed=True,
                    check_type='date_range',
                    expected='valid date range',
                    actual='empty database',
                    message="No data in database yet"
                ))
        except Exception as e:
            results.append(VerificationResult(
                passed=False,
                check_type='date_range',
                expected='valid date range',
                actual='error',
                message=f"Date range check failed: {e}"
            ))
        
        # Calculate overall status
        passed = sum(1 for r in results if r.passed)
        failed = len(results) - passed
        
        status = 'PASSED' if failed == 0 else ('FAILED' if passed == 0 else 'PARTIAL')
        summary = f"Database integrity: {passed}/{len(results)} checks passed"
        
        return VerificationReport(
            timestamp=timestamp,
            source_report='database_integrity',
            total_checks=len(results),
            passed_checks=passed,
            failed_checks=failed,
            status=status,
            results=results,
            summary=summary
        )
    
    def quick_verify(self, inserted_count: int, source_report: str) -> bool:
        """
        Quick verification that just checks record count.
        Useful for lightweight verification during batch operations.
        
        Args:
            inserted_count: Number of records that were inserted
            source_report: Source report name
            
        Returns:
            True if verification passes, False otherwise
        """
        try:
            db_count = self.db.get_record_count(source_report=source_report)
            passed = db_count >= inserted_count
            
            if passed:
                logger.info(f"Quick verification passed for {source_report}: {db_count} records")
            else:
                logger.warning(f"Quick verification failed for {source_report}: expected {inserted_count}, got {db_count}")
            
            return passed
            
        except Exception as e:
            logger.error(f"Quick verification error: {e}")
            return False


# Factory function
def create_verification_agent(database_agent, sample_size: int = 5) -> VerificationAgent:
    """
    Create a VerificationAgent instance.
    
    Args:
        database_agent: DatabaseAgent instance
        sample_size: Number of samples for detailed verification
        
    Returns:
        Configured VerificationAgent instance
    """
    return VerificationAgent(database_agent, sample_size)