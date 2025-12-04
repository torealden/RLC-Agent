"""
Verification Agent for Commodity Data Integrity

This agent verifies that data was loaded correctly into the database
and checks for data quality issues, anomalies, and inconsistencies.
"""

import json
import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple

import pandas as pd
from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Session, sessionmaker

from ..database.models import (
    Commodity, PriceData, FundamentalData, CropProgress, TradeFlow,
    DataLoadLog, QualityAlert, DataSourceType, AlertSeverity, LoadStatus,
    create_quality_alert
)
from ..config.settings import (
    CommodityDatabaseConfig, VerificationConfig, ValidationConfig,
    default_config
)


logger = logging.getLogger(__name__)


# =============================================================================
# RESULT DATACLASSES
# =============================================================================

@dataclass
class VerificationCheck:
    """Result of a single verification check."""
    check_name: str
    passed: bool
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    severity: AlertSeverity = AlertSeverity.INFO


@dataclass
class VerificationResult:
    """Result of verification process."""
    success: bool
    load_id: Optional[int] = None
    source_name: Optional[str] = None
    checks_passed: int = 0
    checks_failed: int = 0
    checks_warning: int = 0
    checks: List[VerificationCheck] = field(default_factory=list)
    alerts_created: int = 0
    duration_seconds: float = 0.0

    def add_check(self, check: VerificationCheck):
        """Add a verification check result."""
        self.checks.append(check)
        if check.passed:
            self.checks_passed += 1
        elif check.severity in [AlertSeverity.ERROR, AlertSeverity.CRITICAL]:
            self.checks_failed += 1
            self.success = False
        else:
            self.checks_warning += 1


@dataclass
class DataQualityReport:
    """Comprehensive data quality report."""
    report_date: datetime = field(default_factory=datetime.utcnow)
    commodity_count: int = 0
    price_record_count: int = 0
    fundamental_record_count: int = 0
    crop_progress_record_count: int = 0
    trade_flow_record_count: int = 0
    data_freshness: Dict[str, Any] = field(default_factory=dict)
    coverage_gaps: List[Dict[str, Any]] = field(default_factory=list)
    anomalies: List[Dict[str, Any]] = field(default_factory=list)
    quality_score: float = 0.0  # 0-100


# =============================================================================
# VERIFICATION AGENT
# =============================================================================

class VerificationAgent:
    """
    Agent for verifying data integrity and quality.

    Features:
    - Record count verification against source
    - Checksum verification
    - Value range validation
    - Statistical anomaly detection
    - Historical comparison
    - Coverage gap detection
    - Data freshness monitoring
    """

    def __init__(
        self,
        config: Optional[CommodityDatabaseConfig] = None,
        session_factory: Optional[sessionmaker] = None
    ):
        """Initialize the Verification Agent."""
        self.config = config or default_config
        self._session_factory = session_factory

    @property
    def session_factory(self) -> sessionmaker:
        """Get session factory."""
        if self._session_factory is None:
            from sqlalchemy import create_engine
            connection_string = self.config.database.get_connection_string()
            engine = create_engine(connection_string)
            self._session_factory = sessionmaker(bind=engine)
        return self._session_factory

    def get_session(self) -> Session:
        """Create a new database session."""
        return self.session_factory()

    # =========================================================================
    # POST-LOAD VERIFICATION
    # =========================================================================

    def verify_load(
        self,
        load_id: int,
        source_data: Optional[Any] = None,
        source_count: Optional[int] = None,
        source_checksum: Optional[str] = None
    ) -> VerificationResult:
        """
        Verify a data load operation.

        Args:
            load_id: ID of the DataLoadLog entry
            source_data: Optional source data for comparison
            source_count: Expected record count from source
            source_checksum: Expected checksum from source

        Returns:
            VerificationResult with all checks
        """
        start_time = datetime.utcnow()
        result = VerificationResult(success=True, load_id=load_id)

        session = self.get_session()
        try:
            # Get load log
            load_log = session.query(DataLoadLog).get(load_id)
            if not load_log:
                result.success = False
                result.add_check(VerificationCheck(
                    check_name="load_exists",
                    passed=False,
                    message=f"Load ID {load_id} not found",
                    severity=AlertSeverity.ERROR
                ))
                return result

            result.source_name = load_log.source_name

            # Check load status
            result.add_check(self._check_load_status(load_log))

            # Verify record counts
            if source_count is not None:
                result.add_check(self._check_record_count(
                    load_log, source_count
                ))

            # Verify no errors
            result.add_check(self._check_error_rate(load_log))

            # Verify data was actually inserted
            result.add_check(self._check_data_exists(session, load_log))

            # Verify value ranges for loaded data
            range_checks = self._check_value_ranges(session, load_log)
            for check in range_checks:
                result.add_check(check)

            # Check for statistical anomalies
            if self.config.verification.verify_statistical_distribution:
                anomaly_checks = self._check_statistical_anomalies(session, load_log)
                for check in anomaly_checks:
                    result.add_check(check)

            # Compare to previous load if configured
            if self.config.verification.compare_to_previous_load:
                comparison_check = self._check_vs_previous_load(session, load_log)
                if comparison_check:
                    result.add_check(comparison_check)

            # Create alerts for failed checks
            for check in result.checks:
                if not check.passed and check.severity in [AlertSeverity.WARNING,
                                                            AlertSeverity.ERROR,
                                                            AlertSeverity.CRITICAL]:
                    alert = create_quality_alert(
                        session,
                        alert_type=f"verification_{check.check_name}",
                        table_name=load_log.load_type,
                        message=check.message,
                        severity=check.severity,
                        details=json.dumps(check.details),
                        load_id=load_id
                    )
                    result.alerts_created += 1

            session.commit()

        except Exception as e:
            session.rollback()
            result.success = False
            result.add_check(VerificationCheck(
                check_name="verification_error",
                passed=False,
                message=f"Verification failed with error: {str(e)}",
                severity=AlertSeverity.ERROR
            ))
            logger.exception("Verification failed")

        finally:
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            session.close()

        return result

    def _check_load_status(self, load_log: DataLoadLog) -> VerificationCheck:
        """Check that load completed successfully."""
        if load_log.status == LoadStatus.SUCCESS:
            return VerificationCheck(
                check_name="load_status",
                passed=True,
                message="Load completed successfully"
            )
        elif load_log.status == LoadStatus.PARTIAL:
            return VerificationCheck(
                check_name="load_status",
                passed=True,
                message=f"Load completed with some errors: {load_log.records_errored} errors",
                severity=AlertSeverity.WARNING
            )
        else:
            return VerificationCheck(
                check_name="load_status",
                passed=False,
                message=f"Load failed: {load_log.error_message}",
                severity=AlertSeverity.ERROR
            )

    def _check_record_count(
        self,
        load_log: DataLoadLog,
        expected_count: int
    ) -> VerificationCheck:
        """Verify record count matches source."""
        actual = load_log.records_inserted + load_log.records_updated
        tolerance = self.config.verification.count_tolerance_pct / 100

        if expected_count == 0:
            passed = actual == 0
        else:
            diff_pct = abs(actual - expected_count) / expected_count
            passed = diff_pct <= tolerance

        return VerificationCheck(
            check_name="record_count",
            passed=passed,
            message=f"Record count: expected {expected_count}, got {actual}",
            details={
                "expected": expected_count,
                "actual": actual,
                "difference": actual - expected_count,
                "difference_pct": (actual - expected_count) / expected_count * 100 if expected_count > 0 else 0
            },
            severity=AlertSeverity.ERROR if not passed else AlertSeverity.INFO
        )

    def _check_error_rate(self, load_log: DataLoadLog) -> VerificationCheck:
        """Check that error rate is acceptable."""
        total = load_log.records_read
        if total == 0:
            return VerificationCheck(
                check_name="error_rate",
                passed=True,
                message="No records to check"
            )

        error_rate = load_log.records_errored / total * 100

        # Allow up to 5% error rate
        passed = error_rate <= 5.0

        return VerificationCheck(
            check_name="error_rate",
            passed=passed,
            message=f"Error rate: {error_rate:.2f}% ({load_log.records_errored} of {total})",
            details={
                "error_count": load_log.records_errored,
                "total_count": total,
                "error_rate_pct": error_rate
            },
            severity=AlertSeverity.ERROR if error_rate > 10 else (
                AlertSeverity.WARNING if not passed else AlertSeverity.INFO
            )
        )

    def _check_data_exists(
        self,
        session: Session,
        load_log: DataLoadLog
    ) -> VerificationCheck:
        """Verify that data was actually inserted into the database."""
        load_type = load_log.load_type
        load_id = load_log.load_id

        # Query appropriate table
        if load_type == "price":
            count = session.query(func.count(PriceData.id)).filter_by(
                load_id=load_id
            ).scalar()
        elif load_type == "fundamental":
            count = session.query(func.count(FundamentalData.id)).filter_by(
                load_id=load_id
            ).scalar()
        elif load_type == "crop_progress":
            count = session.query(func.count(CropProgress.id)).filter_by(
                load_id=load_id
            ).scalar()
        elif load_type == "trade_flow":
            count = session.query(func.count(TradeFlow.id)).filter_by(
                load_id=load_id
            ).scalar()
        else:
            return VerificationCheck(
                check_name="data_exists",
                passed=False,
                message=f"Unknown load type: {load_type}",
                severity=AlertSeverity.ERROR
            )

        expected = load_log.records_inserted
        passed = count >= expected * 0.95  # Allow 5% margin

        return VerificationCheck(
            check_name="data_exists",
            passed=passed,
            message=f"Found {count} records with load_id={load_id} (expected {expected})",
            details={"found": count, "expected": expected},
            severity=AlertSeverity.ERROR if not passed else AlertSeverity.INFO
        )

    def _check_value_ranges(
        self,
        session: Session,
        load_log: DataLoadLog
    ) -> List[VerificationCheck]:
        """Check that values are within expected ranges."""
        checks = []
        load_type = load_log.load_type
        load_id = load_log.load_id
        validation = self.config.validation

        if load_type == "price":
            # Check price ranges
            stats = session.query(
                func.min(PriceData.price),
                func.max(PriceData.price),
                func.avg(PriceData.price)
            ).filter_by(load_id=load_id).first()

            if stats[0] is not None:
                min_price, max_price, avg_price = stats

                # Check minimum
                if float(min_price) < validation.min_valid_price:
                    checks.append(VerificationCheck(
                        check_name="price_min",
                        passed=False,
                        message=f"Price below minimum: {min_price} < {validation.min_valid_price}",
                        severity=AlertSeverity.WARNING
                    ))
                else:
                    checks.append(VerificationCheck(
                        check_name="price_min",
                        passed=True,
                        message=f"Minimum price OK: {min_price}"
                    ))

                # Check maximum
                if float(max_price) > validation.max_valid_price:
                    checks.append(VerificationCheck(
                        check_name="price_max",
                        passed=False,
                        message=f"Price above maximum: {max_price} > {validation.max_valid_price}",
                        severity=AlertSeverity.WARNING
                    ))
                else:
                    checks.append(VerificationCheck(
                        check_name="price_max",
                        passed=True,
                        message=f"Maximum price OK: {max_price}"
                    ))

        elif load_type == "fundamental":
            # Check for stocks_to_use outliers
            stu_records = session.query(FundamentalData).filter(
                FundamentalData.load_id == load_id,
                FundamentalData.field_name == 'stocks_to_use'
            ).all()

            for record in stu_records:
                value = float(record.value)
                if value > validation.max_stocks_to_use:
                    checks.append(VerificationCheck(
                        check_name="stocks_to_use_max",
                        passed=False,
                        message=f"Stocks-to-use ratio unusually high: {value}% for {record.period}",
                        details={"commodity_id": record.commodity_id, "period": record.period},
                        severity=AlertSeverity.WARNING
                    ))

        elif load_type == "crop_progress":
            # Check percentage bounds
            progress_records = session.query(CropProgress).filter_by(load_id=load_id).all()

            for record in progress_records:
                for field in ['pct_planted', 'pct_harvested', 'pct_good_excellent']:
                    value = getattr(record, field)
                    if value is not None:
                        value = float(value)
                        if value < 0 or value > 100:
                            checks.append(VerificationCheck(
                                check_name=f"{field}_range",
                                passed=False,
                                message=f"{field} out of range: {value}% for week {record.week_ending}",
                                severity=AlertSeverity.ERROR
                            ))

        return checks

    def _check_statistical_anomalies(
        self,
        session: Session,
        load_log: DataLoadLog
    ) -> List[VerificationCheck]:
        """Check for statistical anomalies in loaded data."""
        checks = []
        load_type = load_log.load_type
        load_id = load_log.load_id
        std_threshold = self.config.validation.outlier_std_devs
        min_points = self.config.validation.min_data_points_for_stats

        if load_type == "price":
            # Group by commodity and check for outliers
            commodities = session.query(PriceData.commodity_id).filter_by(
                load_id=load_id
            ).distinct().all()

            for (commodity_id,) in commodities:
                # Get historical prices for this commodity
                prices = session.query(PriceData.price).filter_by(
                    commodity_id=commodity_id
                ).order_by(PriceData.observation_date.desc()).limit(100).all()

                if len(prices) >= min_points:
                    values = [float(p[0]) for p in prices]
                    mean = statistics.mean(values)
                    stdev = statistics.stdev(values)

                    # Check new prices against historical distribution
                    new_prices = session.query(PriceData).filter_by(
                        commodity_id=commodity_id,
                        load_id=load_id
                    ).all()

                    for record in new_prices:
                        price = float(record.price)
                        z_score = (price - mean) / stdev if stdev > 0 else 0

                        if abs(z_score) > std_threshold:
                            checks.append(VerificationCheck(
                                check_name="price_outlier",
                                passed=False,
                                message=f"Price outlier detected: {price} (z-score: {z_score:.2f}) on {record.observation_date}",
                                details={
                                    "commodity_id": commodity_id,
                                    "price": price,
                                    "mean": mean,
                                    "stdev": stdev,
                                    "z_score": z_score
                                },
                                severity=AlertSeverity.WARNING
                            ))

        return checks

    def _check_vs_previous_load(
        self,
        session: Session,
        load_log: DataLoadLog
    ) -> Optional[VerificationCheck]:
        """Compare current load to previous load of same type/source."""
        # Find previous load
        previous = session.query(DataLoadLog).filter(
            DataLoadLog.load_type == load_log.load_type,
            DataLoadLog.source_name == load_log.source_name,
            DataLoadLog.load_id < load_log.load_id,
            DataLoadLog.status == LoadStatus.SUCCESS
        ).order_by(DataLoadLog.started_at.desc()).first()

        if not previous:
            return None

        # Compare record counts
        current_count = load_log.records_inserted + load_log.records_updated
        previous_count = previous.records_inserted + previous.records_updated

        if previous_count == 0:
            return None

        change_pct = (current_count - previous_count) / previous_count * 100
        threshold = self.config.verification.large_change_threshold_pct

        if abs(change_pct) > threshold:
            return VerificationCheck(
                check_name="vs_previous_load",
                passed=False,
                message=f"Large change from previous load: {change_pct:.1f}% ({previous_count} -> {current_count})",
                details={
                    "previous_count": previous_count,
                    "current_count": current_count,
                    "change_pct": change_pct,
                    "previous_load_id": previous.load_id
                },
                severity=AlertSeverity.WARNING
            )

        return VerificationCheck(
            check_name="vs_previous_load",
            passed=True,
            message=f"Change from previous load: {change_pct:.1f}%",
            details={
                "previous_count": previous_count,
                "current_count": current_count,
                "change_pct": change_pct
            }
        )

    # =========================================================================
    # DATA QUALITY REPORTING
    # =========================================================================

    def generate_quality_report(self) -> DataQualityReport:
        """Generate a comprehensive data quality report."""
        report = DataQualityReport()

        session = self.get_session()
        try:
            # Record counts
            report.commodity_count = session.query(func.count(Commodity.commodity_id)).scalar()
            report.price_record_count = session.query(func.count(PriceData.id)).scalar()
            report.fundamental_record_count = session.query(func.count(FundamentalData.id)).scalar()
            report.crop_progress_record_count = session.query(func.count(CropProgress.id)).scalar()
            report.trade_flow_record_count = session.query(func.count(TradeFlow.id)).scalar()

            # Data freshness
            report.data_freshness = self._check_data_freshness(session)

            # Coverage gaps
            report.coverage_gaps = self._find_coverage_gaps(session)

            # Recent anomalies
            report.anomalies = self._get_recent_anomalies(session)

            # Calculate quality score
            report.quality_score = self._calculate_quality_score(report, session)

        finally:
            session.close()

        return report

    def _check_data_freshness(self, session: Session) -> Dict[str, Any]:
        """Check when data was last updated for each type."""
        freshness = {}

        # Price data freshness
        latest_price = session.query(func.max(PriceData.observation_date)).scalar()
        if latest_price:
            days_old = (date.today() - latest_price).days
            freshness['price'] = {
                'latest_date': str(latest_price),
                'days_old': days_old,
                'is_stale': days_old > 5  # Stale if > 5 days old
            }

        # Fundamental data freshness
        latest_fundamental = session.query(func.max(FundamentalData.fetched_at)).scalar()
        if latest_fundamental:
            hours_old = (datetime.utcnow() - latest_fundamental).total_seconds() / 3600
            freshness['fundamental'] = {
                'last_fetch': str(latest_fundamental),
                'hours_old': hours_old,
                'is_stale': hours_old > 168  # Stale if > 1 week old
            }

        # Crop progress freshness
        latest_progress = session.query(func.max(CropProgress.week_ending)).scalar()
        if latest_progress:
            days_old = (date.today() - latest_progress).days
            freshness['crop_progress'] = {
                'latest_week': str(latest_progress),
                'days_old': days_old,
                'is_stale': days_old > 14  # Stale if > 2 weeks old
            }

        return freshness

    def _find_coverage_gaps(self, session: Session) -> List[Dict[str, Any]]:
        """Find gaps in data coverage."""
        gaps = []

        # Check for missing recent price data by commodity
        commodities = session.query(Commodity).filter_by(is_active=True).all()

        for commodity in commodities:
            latest = session.query(func.max(PriceData.observation_date)).filter_by(
                commodity_id=commodity.commodity_id
            ).scalar()

            if latest:
                days_since = (date.today() - latest).days
                if days_since > 7:  # Gap if no data for > 7 days
                    gaps.append({
                        'type': 'price_gap',
                        'commodity': commodity.name,
                        'last_date': str(latest),
                        'days_missing': days_since
                    })
            else:
                gaps.append({
                    'type': 'no_price_data',
                    'commodity': commodity.name
                })

            # Check for missing fundamental data for recent periods
            current_year = date.today().year
            for year in [f"{current_year-1}/{str(current_year)[-2:]}",
                         f"{current_year}/{str(current_year+1)[-2:]}"]:
                fundamental_count = session.query(func.count(FundamentalData.id)).filter_by(
                    commodity_id=commodity.commodity_id,
                    period=year
                ).scalar()

                if fundamental_count == 0:
                    gaps.append({
                        'type': 'missing_fundamental',
                        'commodity': commodity.name,
                        'period': year
                    })

        return gaps

    def _get_recent_anomalies(self, session: Session, days: int = 7) -> List[Dict[str, Any]]:
        """Get anomalies detected in recent period."""
        cutoff = datetime.utcnow() - timedelta(days=days)

        alerts = session.query(QualityAlert).filter(
            QualityAlert.created_at >= cutoff,
            QualityAlert.is_resolved == False
        ).order_by(QualityAlert.created_at.desc()).limit(50).all()

        return [
            {
                'alert_id': alert.alert_id,
                'type': alert.alert_type,
                'severity': alert.severity.value,
                'message': alert.message,
                'created_at': str(alert.created_at)
            }
            for alert in alerts
        ]

    def _calculate_quality_score(
        self,
        report: DataQualityReport,
        session: Session
    ) -> float:
        """Calculate overall data quality score (0-100)."""
        score = 100.0
        deductions = []

        # Deduct for stale data
        for data_type, freshness in report.data_freshness.items():
            if freshness.get('is_stale'):
                deduction = 10
                score -= deduction
                deductions.append(f"Stale {data_type} data: -{deduction}")

        # Deduct for coverage gaps (up to 20 points)
        gap_count = len(report.coverage_gaps)
        if gap_count > 0:
            deduction = min(gap_count * 2, 20)
            score -= deduction
            deductions.append(f"{gap_count} coverage gaps: -{deduction}")

        # Deduct for anomalies (up to 15 points)
        anomaly_count = len(report.anomalies)
        if anomaly_count > 0:
            deduction = min(anomaly_count * 3, 15)
            score -= deduction
            deductions.append(f"{anomaly_count} unresolved anomalies: -{deduction}")

        # Deduct for recent failed loads
        recent_failures = session.query(func.count(DataLoadLog.load_id)).filter(
            DataLoadLog.status == LoadStatus.FAILED,
            DataLoadLog.started_at >= datetime.utcnow() - timedelta(days=7)
        ).scalar()

        if recent_failures > 0:
            deduction = min(recent_failures * 5, 15)
            score -= deduction
            deductions.append(f"{recent_failures} recent failed loads: -{deduction}")

        return max(0.0, score)

    # =========================================================================
    # DATA RECONCILIATION
    # =========================================================================

    def reconcile_with_source(
        self,
        source_data: pd.DataFrame,
        data_type: str,
        commodity_name: str = None
    ) -> Dict[str, Any]:
        """
        Reconcile database data with source data.

        Returns detailed comparison of records.
        """
        session = self.get_session()
        try:
            if data_type == "price":
                return self._reconcile_price_data(session, source_data, commodity_name)
            elif data_type == "fundamental":
                return self._reconcile_fundamental_data(session, source_data, commodity_name)
            else:
                return {"error": f"Unsupported data type: {data_type}"}
        finally:
            session.close()

    def _reconcile_price_data(
        self,
        session: Session,
        source_data: pd.DataFrame,
        commodity_name: str = None
    ) -> Dict[str, Any]:
        """Reconcile price data with source."""
        result = {
            "matching": 0,
            "missing_in_db": 0,
            "missing_in_source": 0,
            "value_mismatches": [],
            "details": []
        }

        # Get commodity ID
        if commodity_name:
            commodity = session.query(Commodity).filter_by(name=commodity_name).first()
            if not commodity:
                return {"error": f"Commodity not found: {commodity_name}"}
            commodity_id = commodity.commodity_id
        else:
            commodity_id = None

        # Build lookup from source
        source_lookup = {}
        for _, row in source_data.iterrows():
            key = (str(row.get('observation_date', row.get('date'))),
                   row.get('location', 'National Average'))
            source_lookup[key] = row.to_dict()

        # Query database
        query = session.query(PriceData)
        if commodity_id:
            query = query.filter_by(commodity_id=commodity_id)

        db_lookup = {}
        for record in query.all():
            key = (str(record.observation_date), record.location)
            db_lookup[key] = record

        # Compare
        all_keys = set(source_lookup.keys()) | set(db_lookup.keys())

        for key in all_keys:
            in_source = key in source_lookup
            in_db = key in db_lookup

            if in_source and in_db:
                source_price = float(source_lookup[key].get('price', 0))
                db_price = float(db_lookup[key].price)

                if abs(source_price - db_price) > 0.0001:
                    result["value_mismatches"].append({
                        "date": key[0],
                        "location": key[1],
                        "source_price": source_price,
                        "db_price": db_price,
                        "difference": source_price - db_price
                    })
                else:
                    result["matching"] += 1
            elif in_source:
                result["missing_in_db"] += 1
                result["details"].append({
                    "issue": "missing_in_db",
                    "date": key[0],
                    "location": key[1]
                })
            else:
                result["missing_in_source"] += 1
                result["details"].append({
                    "issue": "missing_in_source",
                    "date": key[0],
                    "location": key[1]
                })

        return result

    def _reconcile_fundamental_data(
        self,
        session: Session,
        source_data: pd.DataFrame,
        commodity_name: str = None
    ) -> Dict[str, Any]:
        """Reconcile fundamental data with source."""
        result = {
            "matching": 0,
            "missing_in_db": 0,
            "missing_in_source": 0,
            "value_mismatches": [],
            "details": []
        }

        if commodity_name:
            commodity = session.query(Commodity).filter_by(name=commodity_name).first()
            if not commodity:
                return {"error": f"Commodity not found: {commodity_name}"}
            commodity_id = commodity.commodity_id
        else:
            commodity_id = None

        # Build source lookup (period, field_name) -> value
        source_lookup = {}
        for _, row in source_data.iterrows():
            period = str(row.get('period', row.get('marketing_year')))
            field_name = row.get('field_name')
            if period and field_name:
                source_lookup[(period, field_name)] = row.to_dict()

        # Query database
        query = session.query(FundamentalData)
        if commodity_id:
            query = query.filter_by(commodity_id=commodity_id)

        db_lookup = {}
        for record in query.all():
            key = (record.period, record.field_name)
            db_lookup[key] = record

        # Compare
        all_keys = set(source_lookup.keys()) | set(db_lookup.keys())

        for key in all_keys:
            in_source = key in source_lookup
            in_db = key in db_lookup

            if in_source and in_db:
                source_value = float(source_lookup[key].get('value', 0))
                db_value = float(db_lookup[key].value)

                tolerance = abs(source_value) * (self.config.verification.value_tolerance_pct / 100)
                if abs(source_value - db_value) > max(tolerance, 0.01):
                    result["value_mismatches"].append({
                        "period": key[0],
                        "field": key[1],
                        "source_value": source_value,
                        "db_value": db_value,
                        "difference": source_value - db_value
                    })
                else:
                    result["matching"] += 1
            elif in_source:
                result["missing_in_db"] += 1
            else:
                result["missing_in_source"] += 1

        return result
