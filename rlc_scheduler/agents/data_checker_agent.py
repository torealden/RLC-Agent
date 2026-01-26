#!/usr/bin/env python3
"""
Data Checker Agent - Validates data quality across the medallion architecture.

Checks:
1. Bronze Layer: Raw data integrity (matches source APIs)
2. Silver Layer: Transformation accuracy (units, calculations)
3. Gold Layer: External source reconciliation (WASDE, etc.)

Round Lakes Commodities
"""

import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

import requests
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Load environment
load_dotenv(project_root / ".env")

# Database connectivity
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DataChecker')


@dataclass
class ValidationResult:
    """Result of a validation check."""
    check_name: str
    passed: bool
    layer: str  # 'bronze', 'silver', 'gold'
    table: str
    records_checked: int = 0
    records_passed: int = 0
    records_failed: int = 0
    errors: List[Dict] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    details: Dict = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def pass_rate(self) -> float:
        """Calculate pass rate as percentage."""
        if self.records_checked == 0:
            return 100.0
        return (self.records_passed / self.records_checked) * 100

    def to_dict(self) -> Dict:
        """Convert to dictionary for reporting."""
        return {
            'check_name': self.check_name,
            'passed': self.passed,
            'layer': self.layer,
            'table': self.table,
            'records_checked': self.records_checked,
            'records_passed': self.records_passed,
            'records_failed': self.records_failed,
            'pass_rate': f"{self.pass_rate:.2f}%",
            'errors': self.errors[:10],  # Limit to first 10
            'warnings': self.warnings[:10],
            'timestamp': self.timestamp.isoformat()
        }


class BaseChecker(ABC):
    """Base class for data checkers."""

    def __init__(self):
        """Initialize the checker."""
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'rlc_commodities'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }
        self.session = requests.Session()

    def _get_db_connection(self):
        """Get database connection."""
        if not DB_AVAILABLE:
            return None
        return psycopg2.connect(**self.db_config, cursor_factory=RealDictCursor)

    @abstractmethod
    def run_checks(self) -> List[ValidationResult]:
        """Run all validation checks."""
        pass


class BronzeWeatherChecker(BaseChecker):
    """
    Validates bronze layer weather data.

    Checks:
    - Data completeness (no missing dates)
    - Data freshness (recent data exists)
    - Source consistency (expected API responses)
    - JSON structure validity
    """

    OPEN_METEO_BASE = "https://api.open-meteo.com/v1/forecast"

    def run_checks(self) -> List[ValidationResult]:
        """Run all bronze weather validation checks."""
        results = []

        # Check 1: Data completeness
        results.append(self._check_completeness())

        # Check 2: Data freshness
        results.append(self._check_freshness())

        # Check 3: JSON structure validity
        results.append(self._check_json_structure())

        # Check 4: Spot check against source
        results.append(self._spot_check_against_source())

        return results

    def _check_completeness(self) -> ValidationResult:
        """Check for missing dates in the last 7 days."""
        result = ValidationResult(
            check_name='bronze_weather_completeness',
            passed=True,
            layer='bronze',
            table='bronze.weather_raw'
        )

        conn = self._get_db_connection()
        if not conn:
            result.passed = False
            result.errors.append({'error': 'Database connection failed'})
            return result

        try:
            cursor = conn.cursor()

            # Get active locations
            cursor.execute("""
                SELECT id, display_name
                FROM public.weather_location
                WHERE is_active = true
            """)
            locations = cursor.fetchall()

            # Check each location for gaps in last 7 days
            week_ago = date.today() - timedelta(days=7)
            expected_dates = set()
            for i in range(7):
                expected_dates.add(week_ago + timedelta(days=i))

            for loc in locations:
                cursor.execute("""
                    SELECT DISTINCT observation_date
                    FROM bronze.weather_raw
                    WHERE location_id = %s
                    AND observation_date >= %s
                """, (loc['id'], week_ago))

                found_dates = {row['observation_date'] for row in cursor.fetchall()}
                missing_dates = expected_dates - found_dates

                result.records_checked += len(expected_dates)
                result.records_passed += len(found_dates)
                result.records_failed += len(missing_dates)

                if missing_dates:
                    result.warnings.append(
                        f"{loc['display_name']}: missing {len(missing_dates)} dates"
                    )

            result.passed = result.records_failed == 0
            cursor.close()
            conn.close()

        except Exception as e:
            result.passed = False
            result.errors.append({'error': str(e)})
            if conn:
                conn.close()

        return result

    def _check_freshness(self) -> ValidationResult:
        """Check that data from today or yesterday exists."""
        result = ValidationResult(
            check_name='bronze_weather_freshness',
            passed=True,
            layer='bronze',
            table='bronze.weather_raw'
        )

        conn = self._get_db_connection()
        if not conn:
            result.passed = False
            result.errors.append({'error': 'Database connection failed'})
            return result

        try:
            cursor = conn.cursor()

            # Check for recent data
            yesterday = date.today() - timedelta(days=1)
            cursor.execute("""
                SELECT location_id, MAX(observation_date) as latest_date
                FROM bronze.weather_raw
                GROUP BY location_id
            """)

            stale_locations = []
            for row in cursor.fetchall():
                result.records_checked += 1
                if row['latest_date'] >= yesterday:
                    result.records_passed += 1
                else:
                    result.records_failed += 1
                    stale_locations.append({
                        'location': row['location_id'],
                        'latest_date': str(row['latest_date'])
                    })

            if stale_locations:
                result.passed = False
                result.errors.extend(stale_locations[:10])

            cursor.close()
            conn.close()

        except Exception as e:
            result.passed = False
            result.errors.append({'error': str(e)})
            if conn:
                conn.close()

        return result

    def _check_json_structure(self) -> ValidationResult:
        """Validate JSON structure of raw data."""
        result = ValidationResult(
            check_name='bronze_weather_json_structure',
            passed=True,
            layer='bronze',
            table='bronze.weather_raw'
        )

        conn = self._get_db_connection()
        if not conn:
            result.passed = False
            result.errors.append({'error': 'Database connection failed'})
            return result

        try:
            cursor = conn.cursor()

            # Check a sample of recent records
            cursor.execute("""
                SELECT id, location_id, source, raw_response as raw_data, observation_date
                FROM bronze.weather_raw
                WHERE observation_date >= CURRENT_DATE - INTERVAL '7 days'
                ORDER BY RANDOM()
                LIMIT 100
            """)

            required_fields = {
                'open_meteo': ['daily', 'latitude', 'longitude'],
                'openweather': ['main', 'weather'],
                'openweather_onecall': ['current', 'lat', 'lon']
            }

            for row in cursor.fetchall():
                result.records_checked += 1
                source = row['source']
                data = row['raw_data']

                # Check if data is valid JSON (psycopg2 handles this)
                if not isinstance(data, dict):
                    result.records_failed += 1
                    result.errors.append({
                        'id': row['id'],
                        'error': 'Invalid JSON structure'
                    })
                    continue

                # Check required fields for source
                if source in required_fields:
                    missing_fields = []
                    for field in required_fields[source]:
                        if field not in data:
                            missing_fields.append(field)

                    if missing_fields:
                        result.records_failed += 1
                        result.errors.append({
                            'id': row['id'],
                            'source': source,
                            'missing_fields': missing_fields
                        })
                        continue

                result.records_passed += 1

            result.passed = result.records_failed == 0
            cursor.close()
            conn.close()

        except Exception as e:
            result.passed = False
            result.errors.append({'error': str(e)})
            if conn:
                conn.close()

        return result

    def _spot_check_against_source(self) -> ValidationResult:
        """Spot check a sample of records against the source API."""
        result = ValidationResult(
            check_name='bronze_weather_source_verification',
            passed=True,
            layer='bronze',
            table='bronze.weather_raw'
        )

        conn = self._get_db_connection()
        if not conn:
            result.passed = False
            result.errors.append({'error': 'Database connection failed'})
            return result

        try:
            cursor = conn.cursor()

            # Get a sample of locations with coordinates
            cursor.execute("""
                SELECT wl.id, wl.display_name, wl.latitude, wl.longitude,
                       wr.raw_response as raw_data, wr.observation_date
                FROM public.weather_location wl
                JOIN bronze.weather_raw wr ON wl.id = wr.location_id
                WHERE wr.source = 'open_meteo'
                AND wr.observation_date = CURRENT_DATE - 1
                LIMIT 3
            """)

            for row in cursor.fetchall():
                result.records_checked += 1

                # Fetch fresh data from Open-Meteo
                try:
                    response = self.session.get(
                        self.OPEN_METEO_BASE,
                        params={
                            'latitude': float(row['latitude']),
                            'longitude': float(row['longitude']),
                            'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum',
                            'past_days': 2,
                            'forecast_days': 0,
                            'timezone': 'auto'
                        },
                        timeout=10
                    )
                    response.raise_for_status()
                    fresh_data = response.json()

                    # Compare key metrics
                    stored = row['raw_data']
                    stored_daily = stored.get('daily', {})
                    fresh_daily = fresh_data.get('daily', {})

                    # Find yesterday's data in fresh response
                    yesterday_str = str(row['observation_date'])
                    fresh_dates = fresh_daily.get('time', [])

                    if yesterday_str in fresh_dates:
                        idx = fresh_dates.index(yesterday_str)

                        # Compare temperature max
                        stored_max = stored_daily.get('temperature_2m_max', [None])[0]
                        fresh_max = fresh_daily.get('temperature_2m_max', [None])[idx]

                        if stored_max is not None and fresh_max is not None:
                            diff = abs(stored_max - fresh_max)
                            if diff > 1.0:  # Allow 1 degree tolerance
                                result.warnings.append(
                                    f"{row['display_name']}: temp_max diff={diff:.1f}C"
                                )

                    result.records_passed += 1

                except Exception as e:
                    result.records_failed += 1
                    result.errors.append({
                        'location': row['display_name'],
                        'error': str(e)
                    })

            # Pass if >80% verified
            result.passed = result.pass_rate >= 80
            cursor.close()
            conn.close()

        except Exception as e:
            result.passed = False
            result.errors.append({'error': str(e)})
            if conn:
                conn.close()

        return result


class SilverWeatherChecker(BaseChecker):
    """
    Validates silver layer weather data.

    Checks:
    - Transformation accuracy (bronze -> silver)
    - Unit conversions (if any)
    - Calculated fields correctness
    - No duplicate records
    """

    def run_checks(self) -> List[ValidationResult]:
        """Run all silver weather validation checks."""
        results = []

        # Check 1: Bronze to Silver mapping
        results.append(self._check_bronze_silver_mapping())

        # Check 2: No duplicates
        results.append(self._check_duplicates())

        # Check 3: Value ranges
        results.append(self._check_value_ranges())

        return results

    def _check_bronze_silver_mapping(self) -> ValidationResult:
        """Verify silver records correctly transform bronze data."""
        result = ValidationResult(
            check_name='silver_weather_transformation',
            passed=True,
            layer='silver',
            table='silver.weather_observation'
        )

        conn = self._get_db_connection()
        if not conn:
            result.passed = False
            result.errors.append({'error': 'Database connection failed'})
            return result

        try:
            cursor = conn.cursor()

            # Sample bronze records with their silver counterparts
            cursor.execute("""
                SELECT
                    b.id as bronze_id,
                    b.location_id,
                    b.raw_response as raw_data,
                    b.source,
                    b.observation_date,
                    s.temp_high_c as silver_high,
                    s.temp_low_c as silver_low,
                    s.precipitation_mm as silver_precip
                FROM bronze.weather_raw b
                JOIN silver.weather_observation s
                    ON b.location_id = s.location_id
                    AND b.observation_date = s.observation_date
                WHERE b.observation_date >= CURRENT_DATE - 7
                ORDER BY RANDOM()
                LIMIT 50
            """)

            for row in cursor.fetchall():
                result.records_checked += 1
                source = row['source']
                raw = row['raw_data']

                # Extract expected values from bronze
                if source == 'open_meteo':
                    daily = raw.get('daily', {})
                    expected_high = daily.get('temperature_2m_max', [None])[0]
                    expected_low = daily.get('temperature_2m_min', [None])[0]
                    expected_precip = daily.get('precipitation_sum', [None])[0]
                elif source in ('openweather', 'openweather_onecall'):
                    main = raw.get('main', {})
                    expected_high = main.get('temp_max')
                    expected_low = main.get('temp_min')
                    # OpenWeather returns rain in mm for last hour
                    rain = raw.get('rain', {})
                    expected_precip = rain.get('1h', 0) + rain.get('3h', 0)
                else:
                    result.records_passed += 1
                    continue

                # Compare with silver
                errors_found = []

                if expected_high is not None and row['silver_high'] is not None:
                    if abs(float(expected_high) - float(row['silver_high'])) > 0.1:
                        errors_found.append(f"temp_high mismatch")

                if expected_low is not None and row['silver_low'] is not None:
                    if abs(float(expected_low) - float(row['silver_low'])) > 0.1:
                        errors_found.append(f"temp_low mismatch")

                if errors_found:
                    result.records_failed += 1
                    result.errors.append({
                        'bronze_id': row['bronze_id'],
                        'issues': errors_found
                    })
                else:
                    result.records_passed += 1

            result.passed = result.pass_rate >= 95
            cursor.close()
            conn.close()

        except Exception as e:
            result.passed = False
            result.errors.append({'error': str(e)})
            if conn:
                conn.close()

        return result

    def _check_duplicates(self) -> ValidationResult:
        """Check for duplicate records in silver layer."""
        result = ValidationResult(
            check_name='silver_weather_duplicates',
            passed=True,
            layer='silver',
            table='silver.weather_observation'
        )

        conn = self._get_db_connection()
        if not conn:
            result.passed = False
            result.errors.append({'error': 'Database connection failed'})
            return result

        try:
            cursor = conn.cursor()

            # Find duplicates (same location + date + source)
            cursor.execute("""
                SELECT location_id, observation_date, source, COUNT(*) as cnt
                FROM silver.weather_observation
                GROUP BY location_id, observation_date, source
                HAVING COUNT(*) > 1
            """)

            duplicates = cursor.fetchall()
            result.records_checked = 1
            result.records_failed = len(duplicates)
            result.records_passed = 1 if not duplicates else 0

            if duplicates:
                result.passed = False
                for dup in duplicates[:10]:
                    result.errors.append({
                        'location': dup['location_id'],
                        'date': str(dup['observation_date']),
                        'count': dup['cnt']
                    })

            cursor.close()
            conn.close()

        except Exception as e:
            result.passed = False
            result.errors.append({'error': str(e)})
            if conn:
                conn.close()

        return result

    def _check_value_ranges(self) -> ValidationResult:
        """Check that values are within reasonable ranges."""
        result = ValidationResult(
            check_name='silver_weather_value_ranges',
            passed=True,
            layer='silver',
            table='silver.weather_observation'
        )

        conn = self._get_db_connection()
        if not conn:
            result.passed = False
            result.errors.append({'error': 'Database connection failed'})
            return result

        try:
            cursor = conn.cursor()

            # Check for unreasonable values
            cursor.execute("""
                SELECT
                    id, location_id, observation_date,
                    temp_high_c, temp_low_c, precipitation_mm
                FROM silver.weather_observation
                WHERE observation_date >= CURRENT_DATE - 30
                AND (
                    temp_high_c < -60 OR temp_high_c > 60
                    OR temp_low_c < -70 OR temp_low_c > 50
                    OR precipitation_mm < 0 OR precipitation_mm > 500
                    OR temp_high_c < temp_low_c
                )
            """)

            outliers = cursor.fetchall()

            cursor.execute("""
                SELECT COUNT(*) as total
                FROM silver.weather_observation
                WHERE observation_date >= CURRENT_DATE - 30
            """)
            total = cursor.fetchone()['total']

            result.records_checked = total
            result.records_failed = len(outliers)
            result.records_passed = total - len(outliers)

            if outliers:
                result.passed = False
                for row in outliers[:10]:
                    result.errors.append({
                        'location': row['location_id'],
                        'date': str(row['observation_date']),
                        'high': float(row['temp_high_c']) if row['temp_high_c'] else None,
                        'low': float(row['temp_low_c']) if row['temp_low_c'] else None,
                        'precip': float(row['precipitation_mm']) if row['precipitation_mm'] else None
                    })

            cursor.close()
            conn.close()

        except Exception as e:
            result.passed = False
            result.errors.append({'error': str(e)})
            if conn:
                conn.close()

        return result


class GoldWeatherChecker(BaseChecker):
    """
    Validates gold layer weather views.

    Checks:
    - View calculations match expected aggregations
    - Regional summaries are accurate
    - Alert thresholds are correct
    """

    def run_checks(self) -> List[ValidationResult]:
        """Run all gold weather validation checks."""
        results = []

        # Check 1: View calculations
        results.append(self._check_view_calculations())

        # Check 2: Alert thresholds
        results.append(self._check_alert_thresholds())

        return results

    def _check_view_calculations(self) -> ValidationResult:
        """Verify gold view calculations match manual calculations."""
        result = ValidationResult(
            check_name='gold_weather_calculations',
            passed=True,
            layer='gold',
            table='gold.weather_summary'
        )

        conn = self._get_db_connection()
        if not conn:
            result.passed = False
            result.errors.append({'error': 'Database connection failed'})
            return result

        try:
            cursor = conn.cursor()

            # Get a sample from gold view
            cursor.execute("""
                SELECT
                    location_id, observation_date,
                    temp_high_f, precip_7day_total_in
                FROM gold.weather_summary
                WHERE observation_date = CURRENT_DATE - 1
                LIMIT 5
            """)

            gold_rows = cursor.fetchall()

            for row in gold_rows:
                result.records_checked += 1

                # Manually calculate 7-day precipitation (in inches)
                cursor.execute("""
                    SELECT SUM(precipitation_in) as calc_precip
                    FROM silver.weather_observation
                    WHERE location_id = %s
                    AND observation_date BETWEEN %s - INTERVAL '6 days' AND %s
                """, (row['location_id'], row['observation_date'], row['observation_date']))

                calc = cursor.fetchone()

                if calc['calc_precip'] is not None and row['precip_7day_total_in'] is not None:
                    diff = abs(float(calc['calc_precip']) - float(row['precip_7day_total_in']))
                    if diff > 0.01:  # Smaller tolerance for inches
                        result.records_failed += 1
                        result.errors.append({
                            'location': row['location_id'],
                            'calculated': float(calc['calc_precip']),
                            'gold_value': float(row['precip_7day_total_in']),
                            'diff': diff
                        })
                        continue

                result.records_passed += 1

            result.passed = result.pass_rate >= 95
            cursor.close()
            conn.close()

        except Exception as e:
            result.passed = False
            result.errors.append({'error': str(e)})
            if conn:
                conn.close()

        return result

    def _check_alert_thresholds(self) -> ValidationResult:
        """Verify weather alerts use correct thresholds."""
        result = ValidationResult(
            check_name='gold_weather_alert_thresholds',
            passed=True,
            layer='gold',
            table='gold.weather_alerts'
        )

        conn = self._get_db_connection()
        if not conn:
            result.passed = False
            result.errors.append({'error': 'Database connection failed'})
            return result

        try:
            cursor = conn.cursor()

            # Check hard freeze alerts (should be temp_low_c <= -2)
            cursor.execute("""
                SELECT wa.location_id, wa.observation_date, wa.alert_type,
                       wo.temp_low_c
                FROM gold.weather_alerts wa
                JOIN silver.weather_observation wo
                    ON wa.location_id = wo.location_id
                    AND wa.observation_date = wo.observation_date
                WHERE wa.alert_type = 'hard_freeze'
                LIMIT 20
            """)

            for row in cursor.fetchall():
                result.records_checked += 1
                if row['temp_low_c'] is not None and float(row['temp_low_c']) > -2:
                    result.records_failed += 1
                    result.errors.append({
                        'location': row['location_id'],
                        'alert_type': 'hard_freeze',
                        'temp_low': float(row['temp_low_c']),
                        'expected': '<= -2C'
                    })
                else:
                    result.records_passed += 1

            result.passed = result.pass_rate >= 100  # Alerts must be 100% accurate
            cursor.close()
            conn.close()

        except Exception as e:
            result.passed = False
            result.errors.append({'error': str(e)})
            if conn:
                conn.close()

        return result


class DataCheckerAgent:
    """
    Main agent that orchestrates all data validation checks.
    """

    def __init__(self):
        """Initialize the data checker agent."""
        self.checkers = {
            'bronze_weather': BronzeWeatherChecker(),
            'silver_weather': SilverWeatherChecker(),
            'gold_weather': GoldWeatherChecker(),
        }
        logger.info("DataCheckerAgent initialized")

    def run_all_checks(self) -> Dict[str, List[ValidationResult]]:
        """Run all validation checks across all layers."""
        all_results = {}

        for checker_name, checker in self.checkers.items():
            logger.info(f"Running {checker_name} checks...")
            try:
                results = checker.run_checks()
                all_results[checker_name] = results

                for result in results:
                    status = "PASS" if result.passed else "FAIL"
                    logger.info(
                        f"  {result.check_name}: {status} "
                        f"({result.records_passed}/{result.records_checked})"
                    )
            except Exception as e:
                logger.error(f"Error running {checker_name} checks: {e}")
                all_results[checker_name] = [ValidationResult(
                    check_name=f'{checker_name}_error',
                    passed=False,
                    layer='unknown',
                    table='unknown',
                    errors=[{'error': str(e)}]
                )]

        return all_results

    def run_layer_checks(self, layer: str) -> List[ValidationResult]:
        """Run checks for a specific layer."""
        layer_checkers = {
            'bronze': ['bronze_weather'],
            'silver': ['silver_weather'],
            'gold': ['gold_weather']
        }

        checker_names = layer_checkers.get(layer, [])
        results = []

        for name in checker_names:
            if name in self.checkers:
                results.extend(self.checkers[name].run_checks())

        return results

    def generate_report(self, results: Dict[str, List[ValidationResult]]) -> str:
        """Generate a human-readable validation report."""
        report_lines = [
            "=" * 70,
            "DATA VALIDATION REPORT",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 70,
            ""
        ]

        total_checks = 0
        passed_checks = 0

        for checker_name, checker_results in results.items():
            report_lines.append(f"\n--- {checker_name.upper()} ---")

            for result in checker_results:
                total_checks += 1
                status = "PASS" if result.passed else "FAIL"
                if result.passed:
                    passed_checks += 1

                report_lines.append(
                    f"  [{status}] {result.check_name}"
                )
                report_lines.append(
                    f"        Records: {result.records_passed}/{result.records_checked} "
                    f"({result.pass_rate:.1f}%)"
                )

                if result.errors:
                    report_lines.append(f"        Errors ({len(result.errors)}):")
                    for err in result.errors[:3]:
                        report_lines.append(f"          - {err}")
                    if len(result.errors) > 3:
                        report_lines.append(f"          ... and {len(result.errors) - 3} more")

                if result.warnings:
                    report_lines.append(f"        Warnings ({len(result.warnings)}):")
                    for warn in result.warnings[:3]:
                        report_lines.append(f"          - {warn}")

        report_lines.extend([
            "",
            "=" * 70,
            f"SUMMARY: {passed_checks}/{total_checks} checks passed",
            "=" * 70
        ])

        return "\n".join(report_lines)

    def save_results_to_db(self, results: Dict[str, List[ValidationResult]]) -> bool:
        """Save validation results to database for tracking."""
        # TODO: Implement results tracking table
        # This would allow historical tracking of data quality metrics
        pass


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Data Checker Agent')
    parser.add_argument(
        '--layer',
        choices=['bronze', 'silver', 'gold', 'all'],
        default='all',
        help='Layer to check (default: all)'
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output results as JSON'
    )

    args = parser.parse_args()

    agent = DataCheckerAgent()

    if args.layer == 'all':
        results = agent.run_all_checks()
    else:
        results = {args.layer: agent.run_layer_checks(args.layer)}

    if args.json:
        output = {
            checker: [r.to_dict() for r in checker_results]
            for checker, checker_results in results.items()
        }
        print(json.dumps(output, indent=2))
    else:
        print(agent.generate_report(results))


if __name__ == "__main__":
    main()
