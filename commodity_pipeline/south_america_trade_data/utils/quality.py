"""
Data Quality Validation Utilities

Provides tools for:
- Record validation
- Outlier detection (z-score, deviation from mean)
- Completeness checking
- Duplicate detection
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import hashlib

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# =============================================================================
# QUALITY ALERT TYPES
# =============================================================================

class AlertSeverity:
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertType:
    """Alert type identifiers"""
    MISSING_REQUIRED = "missing_required_field"
    INVALID_VALUE = "invalid_value"
    OUT_OF_RANGE = "value_out_of_range"
    DUPLICATE = "duplicate_record"
    OUTLIER_ZSCORE = "outlier_zscore"
    OUTLIER_DEVIATION = "outlier_deviation"
    MISSING_PERIOD = "missing_period"
    INCOMPLETE_DATA = "incomplete_data"
    SUM_MISMATCH = "sum_mismatch"


@dataclass
class QualityAlert:
    """A data quality alert"""
    alert_type: str
    severity: str
    message: str
    data_source: Optional[str] = None
    reporter_country: Optional[str] = None
    period: Optional[str] = None
    hs_code: Optional[str] = None
    partner_country: Optional[str] = None
    expected_value: Optional[float] = None
    actual_value: Optional[float] = None
    deviation_pct: Optional[float] = None
    zscore: Optional[float] = None
    record_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)


# =============================================================================
# RECORD VALIDATOR
# =============================================================================

class QualityValidator:
    """
    Validate individual trade records

    Checks:
    - Required fields present
    - Values within acceptable ranges
    - Valid HS codes
    - Valid country codes
    """

    # Required fields for a valid record
    REQUIRED_FIELDS = [
        'reporter_country',
        'flow',
        'period',
        'hs_code',
        'partner_country',
        'value_usd',
    ]

    # Valid flow types
    VALID_FLOWS = ['export', 'import']

    # Value range limits
    MIN_VALUE_USD = 0.0
    MAX_VALUE_USD = 1e12  # $1 trillion per record (reasonable upper bound)

    MIN_QUANTITY_KG = 0.0
    MAX_QUANTITY_KG = 1e12  # 1 trillion kg

    def __init__(self, custom_required_fields: List[str] = None):
        """
        Initialize validator

        Args:
            custom_required_fields: Override default required fields
        """
        self.required_fields = custom_required_fields or self.REQUIRED_FIELDS

    def validate_record(self, record: Dict) -> Tuple[bool, List[QualityAlert]]:
        """
        Validate a single trade record

        Args:
            record: Dictionary with trade data

        Returns:
            Tuple of (is_valid, list of QualityAlert)
        """
        alerts = []

        # Check required fields
        for field in self.required_fields:
            if field not in record or record[field] is None or record[field] == '':
                alerts.append(QualityAlert(
                    alert_type=AlertType.MISSING_REQUIRED,
                    severity=AlertSeverity.ERROR,
                    message=f"Missing required field: {field}",
                    reporter_country=record.get('reporter_country'),
                    period=record.get('period'),
                    hs_code=record.get('hs_code'),
                ))

        # Validate flow type
        flow = record.get('flow')
        if flow and flow not in self.VALID_FLOWS:
            alerts.append(QualityAlert(
                alert_type=AlertType.INVALID_VALUE,
                severity=AlertSeverity.ERROR,
                message=f"Invalid flow type: {flow}",
                actual_value=flow,
            ))

        # Validate value range
        value = record.get('value_usd')
        if value is not None:
            try:
                value = float(value)
                if value < self.MIN_VALUE_USD:
                    alerts.append(QualityAlert(
                        alert_type=AlertType.OUT_OF_RANGE,
                        severity=AlertSeverity.WARNING,
                        message=f"Negative value: {value}",
                        actual_value=value,
                        reporter_country=record.get('reporter_country'),
                        period=record.get('period'),
                    ))
                elif value > self.MAX_VALUE_USD:
                    alerts.append(QualityAlert(
                        alert_type=AlertType.OUT_OF_RANGE,
                        severity=AlertSeverity.WARNING,
                        message=f"Unusually high value: {value}",
                        actual_value=value,
                        reporter_country=record.get('reporter_country'),
                        period=record.get('period'),
                    ))
            except (ValueError, TypeError):
                alerts.append(QualityAlert(
                    alert_type=AlertType.INVALID_VALUE,
                    severity=AlertSeverity.ERROR,
                    message=f"Invalid value_usd: {value}",
                ))

        # Validate quantity
        quantity = record.get('quantity_kg')
        if quantity is not None:
            try:
                quantity = float(quantity)
                if quantity < self.MIN_QUANTITY_KG:
                    alerts.append(QualityAlert(
                        alert_type=AlertType.OUT_OF_RANGE,
                        severity=AlertSeverity.WARNING,
                        message=f"Negative quantity: {quantity}",
                        actual_value=quantity,
                    ))
                elif quantity > self.MAX_QUANTITY_KG:
                    alerts.append(QualityAlert(
                        alert_type=AlertType.OUT_OF_RANGE,
                        severity=AlertSeverity.WARNING,
                        message=f"Unusually high quantity: {quantity}",
                        actual_value=quantity,
                    ))
            except (ValueError, TypeError):
                pass  # Quantity is optional

        # Validate HS code format
        hs_code = record.get('hs_code', '')
        if hs_code:
            clean_hs = ''.join(c for c in str(hs_code) if c.isdigit())
            if len(clean_hs) < 2:
                alerts.append(QualityAlert(
                    alert_type=AlertType.INVALID_VALUE,
                    severity=AlertSeverity.ERROR,
                    message=f"Invalid HS code (too short): {hs_code}",
                ))
            elif len(clean_hs) > 0:
                chapter = int(clean_hs[:2])
                if chapter < 1 or chapter > 99:
                    alerts.append(QualityAlert(
                        alert_type=AlertType.INVALID_VALUE,
                        severity=AlertSeverity.WARNING,
                        message=f"Invalid HS chapter: {chapter}",
                    ))

        is_valid = len([a for a in alerts if a.severity == AlertSeverity.ERROR]) == 0
        return is_valid, alerts

    def validate_batch(self, records: List[Dict]) -> Tuple[List[Dict], List[Dict], List[QualityAlert]]:
        """
        Validate a batch of records

        Args:
            records: List of trade record dictionaries

        Returns:
            Tuple of (valid_records, invalid_records, all_alerts)
        """
        valid_records = []
        invalid_records = []
        all_alerts = []

        for record in records:
            is_valid, alerts = self.validate_record(record)
            all_alerts.extend(alerts)

            if is_valid:
                valid_records.append(record)
            else:
                invalid_records.append(record)

        return valid_records, invalid_records, all_alerts


# =============================================================================
# OUTLIER DETECTION
# =============================================================================

class OutlierDetector:
    """
    Detect outliers in trade data

    Methods:
    - Z-score: Flag values > N standard deviations from mean
    - Deviation: Flag values deviating > X% from trailing average
    - IQR: Flag values outside interquartile range
    """

    def __init__(
        self,
        zscore_threshold: float = 3.0,
        deviation_threshold_pct: float = 20.0,
        min_sample_size: int = 12
    ):
        """
        Initialize outlier detector

        Args:
            zscore_threshold: Z-score threshold for flagging
            deviation_threshold_pct: Percentage deviation threshold
            min_sample_size: Minimum samples needed for statistics
        """
        self.zscore_threshold = zscore_threshold
        self.deviation_threshold_pct = deviation_threshold_pct
        self.min_sample_size = min_sample_size

    def compute_zscore(
        self,
        value: float,
        reference_values: List[float]
    ) -> Optional[float]:
        """
        Compute z-score for a value

        Args:
            value: Value to evaluate
            reference_values: Historical/comparison values

        Returns:
            Z-score or None if insufficient data
        """
        if len(reference_values) < 2:
            return None

        arr = np.array(reference_values)
        mean = np.mean(arr)
        std = np.std(arr)

        if std == 0:
            return 0.0

        return (value - mean) / std

    def check_zscore_outlier(
        self,
        value: float,
        reference_values: List[float]
    ) -> Tuple[bool, Optional[float]]:
        """
        Check if value is an outlier by z-score

        Returns:
            Tuple of (is_outlier, zscore)
        """
        if len(reference_values) < self.min_sample_size:
            return False, None

        zscore = self.compute_zscore(value, reference_values)

        if zscore is None:
            return False, None

        is_outlier = abs(zscore) > self.zscore_threshold
        return is_outlier, zscore

    def check_deviation_outlier(
        self,
        current_value: float,
        trailing_values: List[float]
    ) -> Tuple[bool, Optional[float]]:
        """
        Check if current value deviates significantly from trailing mean

        Returns:
            Tuple of (is_outlier, deviation_pct)
        """
        if len(trailing_values) < self.min_sample_size:
            return False, None

        mean = np.mean(trailing_values)

        if mean == 0:
            return False, None

        deviation_pct = abs((current_value - mean) / mean) * 100

        is_outlier = deviation_pct > self.deviation_threshold_pct
        return is_outlier, deviation_pct

    def detect_outliers_in_series(
        self,
        df: pd.DataFrame,
        value_column: str,
        group_columns: List[str] = None,
        sort_column: str = 'period'
    ) -> pd.DataFrame:
        """
        Detect outliers in a time series of trade values

        Args:
            df: DataFrame with trade data
            value_column: Column containing values to check
            group_columns: Columns to group by (e.g., ['reporter', 'hs_code_6'])
            sort_column: Column to sort by for time series

        Returns:
            DataFrame with outlier flags added
        """
        df = df.copy()
        df['is_outlier_zscore'] = False
        df['is_outlier_deviation'] = False
        df['zscore'] = None
        df['deviation_pct'] = None

        if group_columns:
            groups = df.groupby(group_columns)
        else:
            groups = [(None, df)]

        result_dfs = []

        for group_key, group_df in groups:
            group_df = group_df.sort_values(sort_column)
            values = group_df[value_column].values

            for i in range(len(values)):
                if i < self.min_sample_size:
                    continue

                # Use trailing values
                trailing = values[max(0, i - self.min_sample_size):i]
                current = values[i]

                # Z-score check
                is_zscore_outlier, zscore = self.check_zscore_outlier(current, trailing)
                group_df.iloc[i, group_df.columns.get_loc('is_outlier_zscore')] = is_zscore_outlier
                group_df.iloc[i, group_df.columns.get_loc('zscore')] = zscore

                # Deviation check
                is_dev_outlier, dev_pct = self.check_deviation_outlier(current, trailing)
                group_df.iloc[i, group_df.columns.get_loc('is_outlier_deviation')] = is_dev_outlier
                group_df.iloc[i, group_df.columns.get_loc('deviation_pct')] = dev_pct

            result_dfs.append(group_df)

        return pd.concat(result_dfs, ignore_index=True)

    def generate_outlier_alerts(
        self,
        df: pd.DataFrame,
        value_column: str = 'value_usd'
    ) -> List[QualityAlert]:
        """
        Generate alerts for detected outliers

        Args:
            df: DataFrame with outlier detection results

        Returns:
            List of QualityAlert objects
        """
        alerts = []

        outlier_rows = df[df['is_outlier_zscore'] | df['is_outlier_deviation']]

        for _, row in outlier_rows.iterrows():
            if row.get('is_outlier_zscore', False):
                alerts.append(QualityAlert(
                    alert_type=AlertType.OUTLIER_ZSCORE,
                    severity=AlertSeverity.WARNING,
                    message=f"Value {row[value_column]} has z-score {row['zscore']:.2f}",
                    reporter_country=row.get('reporter_country'),
                    period=row.get('period'),
                    hs_code=row.get('hs_code_6'),
                    actual_value=row[value_column],
                    zscore=row['zscore'],
                ))

            if row.get('is_outlier_deviation', False):
                alerts.append(QualityAlert(
                    alert_type=AlertType.OUTLIER_DEVIATION,
                    severity=AlertSeverity.WARNING,
                    message=f"Value {row[value_column]} deviates {row['deviation_pct']:.1f}% from mean",
                    reporter_country=row.get('reporter_country'),
                    period=row.get('period'),
                    hs_code=row.get('hs_code_6'),
                    actual_value=row[value_column],
                    deviation_pct=row['deviation_pct'],
                ))

        return alerts


# =============================================================================
# COMPLETENESS CHECKER
# =============================================================================

class CompletenessChecker:
    """
    Check data completeness

    Verifies:
    - All expected periods are present
    - No missing partner countries
    - Record counts match expectations
    - Sum totals match official published totals
    """

    def __init__(self):
        pass

    def check_period_completeness(
        self,
        df: pd.DataFrame,
        expected_periods: List[str],
        group_columns: List[str] = None
    ) -> Tuple[List[str], List[QualityAlert]]:
        """
        Check if all expected periods are present

        Args:
            df: DataFrame with period column
            expected_periods: List of expected period strings (YYYY-MM)
            group_columns: Optional grouping columns

        Returns:
            Tuple of (missing_periods, alerts)
        """
        if 'period' not in df.columns:
            return expected_periods, [QualityAlert(
                alert_type=AlertType.MISSING_REQUIRED,
                severity=AlertSeverity.ERROR,
                message="Period column not found in data",
            )]

        present_periods = set(df['period'].unique())
        expected_set = set(expected_periods)

        missing = list(expected_set - present_periods)

        alerts = []
        for period in missing:
            alerts.append(QualityAlert(
                alert_type=AlertType.MISSING_PERIOD,
                severity=AlertSeverity.WARNING,
                message=f"Missing data for period: {period}",
                period=period,
            ))

        return missing, alerts

    def check_sum_consistency(
        self,
        records: List[Dict],
        expected_total: float,
        value_column: str = 'value_usd',
        tolerance_pct: float = 5.0
    ) -> Tuple[bool, float, Optional[QualityAlert]]:
        """
        Check if sum of records matches expected total

        Args:
            records: List of record dictionaries
            expected_total: Expected sum total
            value_column: Column to sum
            tolerance_pct: Acceptable deviation percentage

        Returns:
            Tuple of (is_consistent, actual_sum, alert if inconsistent)
        """
        actual_sum = sum(
            float(r.get(value_column, 0) or 0)
            for r in records
        )

        if expected_total == 0:
            is_consistent = actual_sum == 0
            deviation = 0
        else:
            deviation = abs(actual_sum - expected_total) / expected_total * 100
            is_consistent = deviation <= tolerance_pct

        alert = None
        if not is_consistent:
            alert = QualityAlert(
                alert_type=AlertType.SUM_MISMATCH,
                severity=AlertSeverity.WARNING,
                message=f"Sum mismatch: expected {expected_total}, got {actual_sum} ({deviation:.2f}% deviation)",
                expected_value=expected_total,
                actual_value=actual_sum,
                deviation_pct=deviation,
            )

        return is_consistent, actual_sum, alert

    def check_duplicate_records(
        self,
        records: List[Dict],
        key_fields: List[str] = None
    ) -> Tuple[List[Dict], List[QualityAlert]]:
        """
        Check for duplicate records

        Args:
            records: List of record dictionaries
            key_fields: Fields that define uniqueness

        Returns:
            Tuple of (duplicate_records, alerts)
        """
        if key_fields is None:
            key_fields = ['reporter_country', 'flow', 'period', 'hs_code', 'partner_country']

        seen = {}
        duplicates = []
        alerts = []

        for record in records:
            # Create key from specified fields
            key_values = tuple(str(record.get(f, '')) for f in key_fields)
            key_hash = hashlib.md5(str(key_values).encode()).hexdigest()

            if key_hash in seen:
                duplicates.append(record)
                alerts.append(QualityAlert(
                    alert_type=AlertType.DUPLICATE,
                    severity=AlertSeverity.WARNING,
                    message=f"Duplicate record found: {key_values}",
                    reporter_country=record.get('reporter_country'),
                    period=record.get('period'),
                    hs_code=record.get('hs_code'),
                    partner_country=record.get('partner_country'),
                ))
            else:
                seen[key_hash] = record

        return duplicates, alerts

    def generate_completeness_report(
        self,
        df: pd.DataFrame,
        reporter: str,
        expected_periods: List[str] = None
    ) -> Dict:
        """
        Generate a completeness report for a dataset

        Args:
            df: DataFrame with trade data
            reporter: Reporter country code
            expected_periods: List of expected periods

        Returns:
            Dictionary with completeness metrics
        """
        report = {
            'reporter': reporter,
            'total_records': len(df),
            'unique_periods': df['period'].nunique() if 'period' in df.columns else 0,
            'unique_partners': df['partner_country'].nunique() if 'partner_country' in df.columns else 0,
            'unique_hs_codes': df['hs_code'].nunique() if 'hs_code' in df.columns else 0,
            'total_value_usd': df['value_usd'].sum() if 'value_usd' in df.columns else 0,
            'completeness_score': 0.0,
            'missing_periods': [],
            'issues': [],
        }

        # Check period completeness
        if expected_periods and 'period' in df.columns:
            missing, alerts = self.check_period_completeness(df, expected_periods)
            report['missing_periods'] = missing
            report['issues'].extend([a.message for a in alerts])

            periods_present = len(expected_periods) - len(missing)
            report['completeness_score'] = periods_present / len(expected_periods) * 100

        return report
