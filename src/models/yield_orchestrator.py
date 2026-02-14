"""
Yield Forecast Orchestrator

Weekly execution pipeline that ties feature engineering + prediction model together.

Usage:
    python -m src.models.yield_orchestrator run                      # Weekly forecast
    python -m src.models.yield_orchestrator run --week 26 --year 2025
    python -m src.models.yield_orchestrator train --crop corn
    python -m src.models.yield_orchestrator backtest --crop corn --years 2020-2024
    python -m src.models.yield_orchestrator report --format markdown
    python -m src.models.yield_orchestrator monitor
    python -m src.models.yield_orchestrator check
"""

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

ALL_CROPS = ['corn', 'soybeans', 'winter_wheat', 'cotton']
CROP_DB_MAP = {'corn': 'CORN', 'soybeans': 'SOYBEANS', 'winter_wheat': 'WHEAT_ALL', 'cotton': 'COTTON'}

# Backtest weeks — checkpoints through the growing season
BACKTEST_WEEKS = [18, 22, 26, 30, 34, 38]

# Core Corn Belt / Soybean Belt states for national estimates
CORN_BELT_STATES = ['IA', 'IL', 'NE', 'MN', 'IN', 'OH', 'SD', 'WI', 'MO', 'KS']
SOY_BELT_STATES = ['IL', 'IA', 'MN', 'IN', 'NE', 'OH', 'MO', 'SD', 'ND', 'AR']
WHEAT_STATES = ['KS', 'OK', 'TX', 'CO', 'NE', 'MT', 'ND', 'MN', 'SD']


def get_db_connection():
    """Get PostgreSQL connection."""
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
    password = (
        os.environ.get("RLC_PG_PASSWORD")
        or os.environ.get("DATABASE_PASSWORD")
        or os.environ.get("DB_PASSWORD")
    )
    return psycopg2.connect(
        host=os.environ.get("DATABASE_HOST", "localhost"),
        port=os.environ.get("DATABASE_PORT", "5432"),
        database=os.environ.get("DATABASE_NAME", "rlc_commodities"),
        user=os.environ.get("DATABASE_USER", "postgres"),
        password=password,
    )


class YieldOrchestrator:
    """
    Weekly execution pipeline for yield forecasting.

    Workflow:
    1. Check data freshness (what's been updated since last run)
    2. Run feature engineering for current week
    3. Load/train prediction models
    4. Generate forecasts
    5. Save to gold.yield_forecast
    6. Log run to silver.yield_model_run
    7. Generate monitoring report
    """

    def __init__(self):
        from src.models.yield_feature_engine import YieldFeatureEngine
        from src.models.yield_prediction_model import YieldPredictionModel
        self.feature_engine = YieldFeatureEngine()
        self.model = YieldPredictionModel()

    # ------------------------------------------------------------------
    # RUN — weekly forecast pipeline
    # ------------------------------------------------------------------

    def run(self, week: int = None, year: int = None,
            crops: list = None, states: list = None) -> dict:
        """
        Execute full weekly forecast pipeline.

        Returns summary dict with forecast counts, key changes, alerts.
        """
        start_time = time.time()
        today = date.today()
        if year is None:
            year = today.year
        if week is None:
            week = self._get_current_nass_week()
        if crops is None:
            crops = ALL_CROPS

        logger.info(f"=== Yield Forecast Run: Year {year}, Week {week} ===")

        # Step 1: Check data freshness
        freshness = self._check_data_freshness()
        logger.info("Data freshness check:")
        for source, info in freshness.items():
            logger.info(f"  {source}: {info}")

        # Step 2: Build features
        logger.info("Building features...")
        feature_summary = {}
        for crop in crops:
            crop_states = states or self._get_crop_states(crop)
            crop_rows = 0
            for st in crop_states:
                count = self.feature_engine.build_features(st, crop, year,
                                                           week_start=max(1, week - 2),
                                                           week_end=week)
                crop_rows += count
            feature_summary[crop] = crop_rows
            logger.info(f"  {crop}: {crop_rows} feature rows built across {len(crop_states)} states")

        # Step 3: Generate predictions
        logger.info("Generating predictions...")
        run_id = str(uuid.uuid4())
        all_predictions = []
        prediction_summary = {}

        for crop in crops:
            crop_states = states or self._get_crop_states(crop)
            predictions = self.model.predict(crop, year, week, states=crop_states)

            if predictions:
                all_predictions.extend(predictions)
                prediction_summary[crop] = {
                    'count': len(predictions),
                    'avg_yield': round(np.mean([p.yield_forecast for p in predictions]), 1),
                    'avg_confidence': round(np.mean([p.confidence for p in predictions]), 2),
                }
                logger.info(f"  {crop}: {len(predictions)} predictions, "
                            f"avg yield={prediction_summary[crop]['avg_yield']} bu/ac")
            else:
                prediction_summary[crop] = {'count': 0, 'note': 'No predictions (model not trained or no features)'}
                logger.warning(f"  {crop}: No predictions generated")

        # Step 4: Save predictions
        if all_predictions:
            self._log_run(run_id, crops, week, year, len(all_predictions),
                          time.time() - start_time)
            self.model.save_predictions(all_predictions, run_id=run_id)
            logger.info(f"Saved {len(all_predictions)} predictions to gold.yield_forecast")

        # Step 5: Check for significant changes
        alerts = self._check_alerts(all_predictions)
        if alerts:
            logger.info(f"ALERTS ({len(alerts)}):")
            for alert in alerts:
                logger.info(f"  {alert}")

        duration = time.time() - start_time
        logger.info(f"=== Run complete in {duration:.1f}s ===")

        return {
            'year': year,
            'week': week,
            'run_id': run_id,
            'features': feature_summary,
            'predictions': prediction_summary,
            'total_predictions': len(all_predictions),
            'alerts': alerts,
            'duration_sec': round(duration, 1),
        }

    # ------------------------------------------------------------------
    # TRAIN — train/retrain models
    # ------------------------------------------------------------------

    def train(self, crops: list = None,
              train_years: range = range(2005, 2024),
              target_weeks: list = None) -> dict:
        """
        Train/retrain all models.

        Builds historical features if needed, then trains models per crop.
        """
        if crops is None:
            crops = ALL_CROPS
        if target_weeks is None:
            target_weeks = [30]  # Default: mid-season peak

        logger.info(f"=== Training Models ===")
        logger.info(f"Crops: {crops}, Years: {min(train_years)}-{max(train_years)}")

        results = {}
        for crop in crops:
            logger.info(f"\nTraining {crop}...")
            crop_results = {}
            for week in target_weeks:
                metrics = self.model.train(crop, train_years=train_years, target_week=week)
                crop_results[f'week_{week}'] = metrics
                if 'error' not in metrics:
                    logger.info(f"  Week {week}: RMSE={metrics.get('rmse_cv')}, "
                                f"MAE={metrics.get('mae_cv')}, R²={metrics.get('r2_cv')}")
                else:
                    logger.warning(f"  Week {week}: {metrics.get('error')} "
                                   f"(n={metrics.get('n_samples')})")
            results[crop] = crop_results

        logger.info(f"\n=== Training Complete ===")
        return results

    # ------------------------------------------------------------------
    # BACKTEST — historical validation
    # ------------------------------------------------------------------

    def backtest(self, test_years: range = range(2020, 2025),
                 crops: list = None) -> dict:
        """
        Run backtesting over historical years.

        For each test year, trains on all other years, predicts at multiple
        weeks, and compares to actual yield.
        """
        if crops is None:
            crops = ['corn', 'soybeans']

        logger.info(f"=== Backtesting ===")
        logger.info(f"Test years: {min(test_years)}-{max(test_years)}, Crops: {crops}")

        conn = get_db_connection()
        cur = conn.cursor()
        results = {}

        try:
            for crop in crops:
                crop_db = CROP_DB_MAP.get(crop, crop.upper())
                logger.info(f"\nBacktesting {crop}...")
                week_errors = {w: [] for w in BACKTEST_WEEKS}

                for test_year in test_years:
                    # Get actual yields for this year
                    cur.execute("""
                        SELECT state_abbrev, yield_per_acre
                        FROM bronze.nass_state_yields
                        WHERE commodity = %s AND year = %s
                          AND yield_per_acre IS NOT NULL AND state_abbrev IS NOT NULL
                    """, (crop_db, test_year))
                    actuals = {r[0]: float(r[1]) for r in cur.fetchall()}

                    if not actuals:
                        logger.warning(f"  No actuals for {crop} {test_year}")
                        continue

                    # Train on all years except test year
                    all_years = list(range(2005, max(test_years) + 1))
                    train_yrs = [y for y in all_years if y != test_year]

                    for week in BACKTEST_WEEKS:
                        # Check if features exist for this year/week
                        cur.execute("""
                            SELECT COUNT(*) FROM silver.yield_features
                            WHERE crop = %s AND year = %s AND week = %s
                        """, (crop, test_year, week))
                        feat_count = cur.fetchone()[0]

                        if feat_count == 0:
                            continue

                        metrics = self.model.train(crop, train_years=range(min(train_yrs), max(train_yrs) + 1),
                                                   target_week=week)
                        if 'error' in metrics:
                            continue

                        predictions = self.model.predict(crop, test_year, week)
                        for p in predictions:
                            if p.state in actuals:
                                error = p.yield_forecast - actuals[p.state]
                                week_errors[week].append({
                                    'year': test_year,
                                    'state': p.state,
                                    'predicted': p.yield_forecast,
                                    'actual': actuals[p.state],
                                    'error': error,
                                })

                # Compute metrics per week
                crop_metrics = {}
                for week in BACKTEST_WEEKS:
                    errors = week_errors[week]
                    if not errors:
                        crop_metrics[f'week_{week}'] = {'n': 0, 'note': 'no data'}
                        continue

                    errs = np.array([e['error'] for e in errors])
                    acts = np.array([e['actual'] for e in errors])
                    preds = np.array([e['predicted'] for e in errors])

                    # Directional accuracy (above/below mean)
                    mean_yield = np.mean(acts)
                    dir_correct = np.sum((preds > mean_yield) == (acts > mean_yield))
                    dir_accuracy = dir_correct / len(acts)

                    crop_metrics[f'week_{week}'] = {
                        'n': len(errors),
                        'rmse': round(float(np.sqrt(np.mean(errs ** 2))), 2),
                        'mae': round(float(np.mean(np.abs(errs))), 2),
                        'mean_error': round(float(np.mean(errs)), 2),
                        'dir_accuracy': round(float(dir_accuracy), 3),
                    }
                    logger.info(f"  Week {week}: n={len(errors)}, "
                                f"RMSE={crop_metrics[f'week_{week}']['rmse']}, "
                                f"MAE={crop_metrics[f'week_{week}']['mae']}, "
                                f"Dir={crop_metrics[f'week_{week}']['dir_accuracy']}")

                results[crop] = crop_metrics

        finally:
            cur.close()
            conn.close()

        logger.info(f"\n=== Backtesting Complete ===")
        return results

    # ------------------------------------------------------------------
    # REPORT — generate yield forecast report
    # ------------------------------------------------------------------

    def report(self, year: int = None, crop: str = None,
               format: str = 'markdown') -> str:
        """Generate yield forecast report."""
        if year is None:
            year = date.today().year

        conn = get_db_connection()
        cur = conn.cursor()

        try:
            week = self._get_current_nass_week()
            crops = [crop] if crop else ALL_CROPS
            report_lines = []

            if format == 'markdown':
                report_lines.append(f"# Yield Forecast Report — Week {week}, {year}\n")

                # National estimates section
                report_lines.append("## National Estimates\n")
                report_lines.append("| Crop | Yield (bu/ac) | vs Trend | vs Last Year | Confidence | Primary Driver |")
                report_lines.append("|------|:---:|:---:|:---:|:---:|---|")

                for c in crops:
                    crop_db = CROP_DB_MAP.get(c, c.upper())
                    cur.execute("""
                        SELECT
                            AVG(yield_forecast) as avg_yield,
                            AVG(vs_trend_pct) as avg_vs_trend,
                            AVG(vs_last_year_pct) as avg_vs_ly,
                            AVG(confidence) as avg_conf,
                            MODE() WITHIN GROUP (ORDER BY primary_driver) as top_driver
                        FROM gold.yield_forecast
                        WHERE commodity = %s AND year = %s AND forecast_week = %s
                          AND model_type = 'ensemble'
                    """, (crop_db, year, week))
                    row = cur.fetchone()
                    if row and row[0] is not None:
                        yield_val = f"{float(row[0]):.1f}"
                        vs_trend = f"{float(row[1]):+.1f}%" if row[1] else "N/A"
                        vs_ly = f"{float(row[2]):+.1f}%" if row[2] else "N/A"
                        conf = f"{float(row[3]):.2f}" if row[3] else "N/A"
                        driver = row[4] or "N/A"
                        report_lines.append(f"| {c.replace('_', ' ').title()} | {yield_val} | {vs_trend} | {vs_ly} | {conf} | {driver} |")
                    else:
                        report_lines.append(f"| {c.replace('_', ' ').title()} | No forecast | — | — | — | — |")

                # Week-over-week changes
                report_lines.append("\n## Week-over-Week Changes\n")
                for c in crops:
                    crop_db = CROP_DB_MAP.get(c, c.upper())
                    cur.execute("""
                        SELECT AVG(wow_change)
                        FROM gold.yield_forecast
                        WHERE commodity = %s AND year = %s AND forecast_week = %s
                          AND model_type = 'ensemble' AND wow_change IS NOT NULL
                    """, (crop_db, year, week))
                    row = cur.fetchone()
                    if row and row[0] is not None:
                        change = float(row[0])
                        direction = "UP" if change > 0 else "DOWN" if change < 0 else "unchanged"
                        if abs(change) < 0.05:
                            report_lines.append(f"- **{c.replace('_', ' ').title()}** forecast unchanged")
                        else:
                            report_lines.append(f"- **{c.replace('_', ' ').title()}** forecast {direction} {abs(change):.1f} bu/acre")

                # Key risk factors
                report_lines.append("\n## Key Risk Factors\n")
                cur.execute("""
                    SELECT commodity, state, risk_level, stress_days_drought,
                           stress_days_heat, precip_vs_normal_pct, growth_stage
                    FROM gold.yield_monitor
                    WHERE year = %s AND forecast_week = %s
                      AND risk_level != 'NORMAL'
                    ORDER BY
                        CASE WHEN risk_level LIKE 'HIGH%%' THEN 1
                             WHEN risk_level LIKE 'ELEVATED%%' THEN 2
                             WHEN risk_level LIKE 'MODERATE%%' THEN 3
                             ELSE 4 END
                    LIMIT 5
                """, (year, week))
                risk_rows = cur.fetchall()
                if risk_rows:
                    for i, (commodity, state, risk, drought, heat, precip, stage) in enumerate(risk_rows, 1):
                        report_lines.append(f"{i}. **{state} {commodity}**: {risk}")
                else:
                    report_lines.append("No elevated risk factors detected.")

                # State highlights
                report_lines.append("\n## State Highlights\n")
                report_lines.append("| State | Crop | Yield | vs Trend | Alert |")
                report_lines.append("|---|---|:---:|:---:|---|")

                cur.execute("""
                    SELECT state, commodity, yield_forecast, vs_trend_pct, primary_driver
                    FROM gold.yield_forecast
                    WHERE year = %s AND forecast_week = %s AND model_type = 'ensemble'
                    ORDER BY ABS(vs_trend_pct) DESC NULLS LAST
                    LIMIT 10
                """, (year, week))
                for state, commodity, yld, vs_trend, driver in cur.fetchall():
                    vs_t = f"{float(vs_trend):+.1f}%" if vs_trend else "N/A"
                    report_lines.append(f"| {state} | {commodity} | {float(yld):.1f} | {vs_t} | {driver or ''} |")

            elif format == 'json':
                # JSON format
                data = {'year': year, 'week': week, 'crops': {}}
                for c in crops:
                    crop_db = CROP_DB_MAP.get(c, c.upper())
                    cur.execute("""
                        SELECT state, yield_forecast, yield_low, yield_high,
                               vs_trend_pct, confidence, primary_driver
                        FROM gold.yield_forecast
                        WHERE commodity = %s AND year = %s AND forecast_week = %s
                          AND model_type = 'ensemble'
                        ORDER BY state
                    """, (crop_db, year, week))
                    data['crops'][c] = [
                        {'state': r[0], 'yield': float(r[1]), 'low': float(r[2]) if r[2] else None,
                         'high': float(r[3]) if r[3] else None, 'vs_trend_pct': float(r[4]) if r[4] else None,
                         'confidence': float(r[5]) if r[5] else None, 'driver': r[6]}
                        for r in cur.fetchall()
                    ]
                return json.dumps(data, indent=2)

            else:
                # Plain text
                report_lines.append(f"Yield Forecast Report — Week {week}, {year}")
                report_lines.append("=" * 50)
                for c in crops:
                    crop_db = CROP_DB_MAP.get(c, c.upper())
                    cur.execute("""
                        SELECT state, yield_forecast, vs_trend_pct, confidence, primary_driver
                        FROM gold.yield_forecast
                        WHERE commodity = %s AND year = %s AND forecast_week = %s
                          AND model_type = 'ensemble'
                        ORDER BY state
                    """, (crop_db, year, week))
                    rows = cur.fetchall()
                    report_lines.append(f"\n{c.upper()} ({len(rows)} states):")
                    for state, yld, vs_t, conf, driver in rows:
                        vs_str = f"{float(vs_t):+.1f}%" if vs_t else "N/A"
                        report_lines.append(f"  {state}: {float(yld):.1f} bu/ac ({vs_str} vs trend) [{driver}]")

            output = "\n".join(report_lines)
            return output

        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # MONITOR — dashboard view
    # ------------------------------------------------------------------

    def monitor(self) -> str:
        """Print dashboard summary from gold.yield_monitor."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            year = date.today().year

            # Get latest forecast week
            cur.execute("""
                SELECT MAX(forecast_week) FROM gold.yield_forecast
                WHERE year = %s AND model_type = 'ensemble'
            """, (year,))
            row = cur.fetchone()
            if not row or row[0] is None:
                return "No forecasts found for current year."

            latest_week = row[0]

            lines = []
            lines.append(f"\n{'='*80}")
            lines.append(f"  YIELD MONITOR — Year {year}, Week {latest_week}")
            lines.append(f"{'='*80}\n")

            # Summary by crop
            cur.execute("""
                SELECT commodity,
                       COUNT(*) as states,
                       ROUND(AVG(yield_forecast)::numeric, 1) as avg_yield,
                       ROUND(AVG(vs_trend_pct)::numeric, 1) as avg_vs_trend,
                       ROUND(AVG(confidence)::numeric, 2) as avg_conf,
                       COUNT(*) FILTER (WHERE risk_level != 'NORMAL') as risk_count
                FROM gold.yield_monitor
                WHERE year = %s AND forecast_week = %s
                GROUP BY commodity
                ORDER BY commodity
            """, (year, latest_week))
            rows = cur.fetchall()

            if not rows:
                lines.append("  No data in yield_monitor view.")
            else:
                lines.append(f"  {'Crop':<16} {'States':>6} {'Avg Yield':>10} {'vs Trend':>9} {'Confidence':>11} {'Risks':>6}")
                lines.append(f"  {'-'*16} {'-'*6} {'-'*10} {'-'*9} {'-'*11} {'-'*6}")
                for commodity, states, avg_yld, vs_trend, conf, risks in rows:
                    vs_str = f"{float(vs_trend):+.1f}%" if vs_trend else "N/A"
                    lines.append(f"  {commodity:<16} {states:>6} {float(avg_yld):>10.1f} {vs_str:>9} {float(conf):>11.2f} {risks:>6}")

                # Risk alerts
                cur.execute("""
                    SELECT commodity, state, risk_level
                    FROM gold.yield_monitor
                    WHERE year = %s AND forecast_week = %s AND risk_level != 'NORMAL'
                    ORDER BY CASE WHEN risk_level LIKE 'HIGH%%' THEN 1
                                  WHEN risk_level LIKE 'ELEVATED%%' THEN 2
                                  ELSE 3 END
                """, (year, latest_week))
                alerts = cur.fetchall()
                if alerts:
                    lines.append(f"\n  RISK ALERTS:")
                    for commodity, state, risk in alerts:
                        lines.append(f"    [{state}] {commodity}: {risk}")

            lines.append(f"\n{'='*80}")
            return "\n".join(lines)

        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # CHECK — data freshness
    # ------------------------------------------------------------------

    def check(self) -> dict:
        """Check and print data freshness for all sources."""
        freshness = self._check_data_freshness()

        lines = []
        lines.append(f"\n{'='*60}")
        lines.append(f"  DATA FRESHNESS CHECK — {date.today()}")
        lines.append(f"{'='*60}\n")

        for source, info in freshness.items():
            lines.append(f"  {source}:")
            for k, v in info.items():
                lines.append(f"    {k}: {v}")
            lines.append("")

        lines.append(f"{'='*60}")
        print("\n".join(lines))
        return freshness

    # ------------------------------------------------------------------
    # INTERNAL HELPERS
    # ------------------------------------------------------------------

    def _check_data_freshness(self) -> dict:
        """Check when each data source was last updated."""
        conn = get_db_connection()
        cur = conn.cursor()
        freshness = {}

        try:
            # Weather observations
            cur.execute("""
                SELECT MAX(observation_date) as latest,
                       COUNT(*) FILTER (WHERE observation_date > CURRENT_DATE - interval '7 days') as rows_7d
                FROM silver.weather_observation
            """)
            row = cur.fetchone()
            freshness['weather_obs'] = {
                'latest': str(row[0]) if row[0] else 'none',
                'rows_7d': row[1] or 0,
            }

            # CPC gridded
            cur.execute("""
                SELECT MAX(year) as latest_year, MAX(nass_week) as latest_week
                FROM bronze.cpc_file_manifest
                WHERE qa_passed = true
            """)
            row = cur.fetchone()
            freshness['cpc_gridded'] = {
                'latest_year': row[0] if row[0] else 'none',
                'latest_week': row[1] if row[1] else 'none',
            }

            # NASS crop progress
            cur.execute("""
                SELECT MAX(week_ending) as latest
                FROM bronze.nass_crop_progress
            """)
            row = cur.fetchone()
            freshness['nass_progress'] = {
                'latest_week_ending': str(row[0]) if row[0] else 'none',
            }

            # NASS condition
            cur.execute("""
                SELECT MAX(week_ending) as latest
                FROM bronze.nass_crop_condition
            """)
            row = cur.fetchone()
            freshness['nass_condition'] = {
                'latest_week_ending': str(row[0]) if row[0] else 'none',
            }

            # World Weather emails
            cur.execute("""
                SELECT MAX(email_date)::date as latest,
                       COUNT(*) FILTER (WHERE email_date > NOW() - interval '7 days') as count_7d
                FROM bronze.weather_email_extract
            """)
            row = cur.fetchone()
            freshness['world_weather'] = {
                'latest_email': str(row[0]) if row[0] else 'none',
                'count_7d': row[1] or 0,
            }

            # Yield features
            cur.execute("""
                SELECT MAX(updated_at)::date as latest, COUNT(*) as total_rows
                FROM silver.yield_features
            """)
            row = cur.fetchone()
            freshness['yield_features'] = {
                'latest': str(row[0]) if row[0] else 'none',
                'total_rows': row[1] or 0,
            }

            # Yield forecasts
            cur.execute("""
                SELECT MAX(created_at)::date as latest, COUNT(*) as total_rows,
                       MAX(forecast_week) as latest_week
                FROM gold.yield_forecast
                WHERE year = %s
            """, (date.today().year,))
            row = cur.fetchone()
            freshness['yield_forecasts'] = {
                'latest': str(row[0]) if row[0] else 'none',
                'total_rows': row[1] or 0,
                'latest_week': row[2] if row[2] else 'none',
            }

        except Exception as e:
            logger.error(f"Error checking data freshness: {e}")

        finally:
            cur.close()
            conn.close()

        return freshness

    def _get_current_nass_week(self) -> int:
        """Map current date to NASS week number."""
        from src.models.yield_feature_engine import date_to_nass_week
        return date_to_nass_week(date.today())

    def _get_crop_states(self, crop: str) -> list:
        """Get list of states that grow a given crop (from yield data)."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            crop_db = CROP_DB_MAP.get(crop, crop.upper())
            cur.execute("""
                SELECT DISTINCT state_abbrev
                FROM bronze.nass_state_yields
                WHERE commodity = %s AND state_abbrev IS NOT NULL AND yield_per_acre IS NOT NULL
                ORDER BY state_abbrev
            """, (crop_db,))
            return [r[0] for r in cur.fetchall()]
        finally:
            cur.close()
            conn.close()

    def _log_run(self, run_id: str, crops: list, week: int, year: int,
                 n_predictions: int, duration: float):
        """Log execution to silver.yield_model_run."""
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO silver.yield_model_run
                    (run_id, model_version, model_type, crops_processed,
                     forecast_week, feature_count, duration_sec, notes)
                VALUES (%s, 'v1', 'ensemble', %s, %s, %s, %s, %s)
            """, (
                run_id, ','.join(crops), week, n_predictions,
                round(duration, 1),
                f"Year {year}, {n_predictions} state-level predictions"
            ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error logging run: {e}")
        finally:
            cur.close()
            conn.close()

    def _check_alerts(self, predictions) -> list:
        """Check for significant week-over-week changes."""
        alerts = []
        for p in predictions:
            # Large wow change
            if hasattr(p, 'vs_trend_pct') and p.vs_trend_pct is not None:
                if abs(p.vs_trend_pct) > 10:
                    alerts.append(
                        f"LARGE DEVIATION: {p.state} {p.commodity} yield "
                        f"{p.yield_forecast:.1f} bu/ac ({p.vs_trend_pct:+.1f}% vs trend) — {p.primary_driver}"
                    )

            # Drought/heat stress during critical period
            if p.primary_driver in ('Drought stress', 'Heat stress'):
                alerts.append(
                    f"STRESS ALERT: {p.state} {p.commodity} — {p.primary_driver}"
                )

        # Deduplicate
        return list(dict.fromkeys(alerts))[:20]


def main():
    parser = argparse.ArgumentParser(description="Yield Forecast Orchestrator")
    subparsers = parser.add_subparsers(dest='command')

    # Run forecast
    run_parser = subparsers.add_parser('run', help='Run weekly forecast')
    run_parser.add_argument('--week', type=int, help='NASS week (default: current)')
    run_parser.add_argument('--year', type=int, help='Year (default: current)')
    run_parser.add_argument('--crop', type=str, help='Specific crop')
    run_parser.add_argument('--state', type=str, help='Specific state')

    # Train models
    train_parser = subparsers.add_parser('train', help='Train models')
    train_parser.add_argument('--years', type=str, default='2005-2023',
                              help='Training year range (e.g., 2005-2023)')
    train_parser.add_argument('--crop', type=str, help='Specific crop')
    train_parser.add_argument('--week', type=int, nargs='+', default=[30],
                              help='Target forecast week(s)')

    # Backtest
    bt_parser = subparsers.add_parser('backtest', help='Run backtesting')
    bt_parser.add_argument('--years', type=str, default='2020-2024',
                           help='Test year range')
    bt_parser.add_argument('--crop', type=str, help='Specific crop')

    # Report
    report_parser = subparsers.add_parser('report', help='Generate report')
    report_parser.add_argument('--format', choices=['markdown', 'text', 'json'],
                               default='markdown')
    report_parser.add_argument('--crop', type=str)
    report_parser.add_argument('--year', type=int)

    # Monitor
    subparsers.add_parser('monitor', help='Dashboard view')

    # Data check
    subparsers.add_parser('check', help='Check data freshness')

    # Add --verbose to all subparsers
    for sp_name, sp in subparsers._name_parser_map.items():
        sp.add_argument('--verbose', '-v', action='store_true')

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    orch = YieldOrchestrator()

    if args.command == 'run':
        crops = [args.crop] if args.crop else None
        states = [args.state] if args.state else None
        result = orch.run(week=args.week, year=args.year, crops=crops, states=states)
        print(f"\nRun Summary:")
        print(f"  Year: {result['year']}, Week: {result['week']}")
        print(f"  Total predictions: {result['total_predictions']}")
        for crop, info in result['predictions'].items():
            if isinstance(info, dict) and 'count' in info:
                print(f"  {crop}: {info['count']} predictions"
                      + (f", avg={info.get('avg_yield')} bu/ac" if info.get('avg_yield') else ""))
        if result['alerts']:
            print(f"\n  Alerts ({len(result['alerts'])}):")
            for a in result['alerts']:
                print(f"    {a}")
        print(f"\n  Duration: {result['duration_sec']}s")

    elif args.command == 'train':
        start, end = map(int, args.years.split('-'))
        crops = [args.crop] if args.crop else None
        result = orch.train(crops=crops, train_years=range(start, end + 1),
                            target_weeks=args.week)
        print("\nTraining Summary:")
        for crop, weeks in result.items():
            print(f"\n  {crop}:")
            for week_key, metrics in weeks.items():
                if 'error' not in metrics:
                    print(f"    {week_key}: RMSE={metrics.get('rmse_cv')}, "
                          f"MAE={metrics.get('mae_cv')}, R²={metrics.get('r2_cv')}, "
                          f"n={metrics.get('n_samples')}")
                else:
                    print(f"    {week_key}: {metrics.get('error')}")

    elif args.command == 'backtest':
        start, end = map(int, args.years.split('-'))
        crops = [args.crop] if args.crop else None
        result = orch.backtest(test_years=range(start, end + 1), crops=crops)
        print("\nBacktest Results:")
        for crop, weeks in result.items():
            print(f"\n  {crop}:")
            print(f"  {'Week':>6} {'N':>5} {'RMSE':>7} {'MAE':>7} {'Bias':>7} {'Dir Acc':>8}")
            print(f"  {'-'*6} {'-'*5} {'-'*7} {'-'*7} {'-'*7} {'-'*8}")
            for week_key, metrics in sorted(weeks.items()):
                if metrics.get('n', 0) > 0:
                    print(f"  {week_key:>6} {metrics['n']:>5} {metrics['rmse']:>7.2f} "
                          f"{metrics['mae']:>7.2f} {metrics['mean_error']:>7.2f} "
                          f"{metrics['dir_accuracy']:>7.1%}")

    elif args.command == 'report':
        output = orch.report(year=args.year, crop=args.crop, format=args.format)
        print(output)

    elif args.command == 'monitor':
        output = orch.monitor()
        print(output)

    elif args.command == 'check':
        orch.check()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
