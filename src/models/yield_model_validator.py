"""
Yield Model Validation Framework

Backtesting, bias analysis, skill scoring, and revision tracking for the
yield prediction model.

Usage:
    python -m src.models.yield_model_validator backtest --crop corn --years 2020-2024
    python -m src.models.yield_model_validator skill --crop corn
    python -m src.models.yield_model_validator bias --crop corn
    python -m src.models.yield_model_validator revisions --crop corn --year 2025
    python -m src.models.yield_model_validator report --crop corn
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

CROP_DB_MAP = {'corn': 'CORN', 'soybeans': 'SOYBEANS', 'winter_wheat': 'WHEAT_ALL', 'cotton': 'COTTON'}
ALL_CROPS = ['corn', 'soybeans', 'winter_wheat', 'cotton']
BACKTEST_WEEKS = [18, 22, 26, 30, 34, 38]

# Accuracy targets by week (RMSE in bu/acre for corn)
ACCURACY_TARGETS_CORN = {
    18: 15.0, 22: 12.0, 26: 10.0, 30: 7.0, 34: 5.0, 38: 4.0,
}


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


class YieldModelValidator:
    """
    Validates yield forecast model performance through backtesting,
    bias analysis, and comparison to benchmarks.
    """

    def __init__(self):
        from src.models.yield_prediction_model import YieldPredictionModel
        self.model = YieldPredictionModel()

    # ------------------------------------------------------------------
    # BACKTESTING
    # ------------------------------------------------------------------

    def run_backtest(self, crop: str, test_years: list,
                     forecast_weeks: list = None) -> dict:
        """
        Leave-one-year-out backtesting.

        For each test year: train on all other years, predict at each
        forecast_week, compare to actual final yield.

        Returns metrics by week.
        """
        if forecast_weeks is None:
            forecast_weeks = BACKTEST_WEEKS

        crop_db = CROP_DB_MAP.get(crop, crop.upper())
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            # Get actual yields
            cur.execute("""
                SELECT year, state_abbrev, yield_per_acre
                FROM bronze.nass_state_yields
                WHERE commodity = %s AND yield_per_acre IS NOT NULL
                  AND state_abbrev IS NOT NULL
                ORDER BY year, state_abbrev
            """, (crop_db,))
            actuals_by_year = {}
            for year, state, yld in cur.fetchall():
                actuals_by_year.setdefault(year, {})[state] = float(yld)

            all_years = sorted(actuals_by_year.keys())
            week_results = {w: {'predictions': [], 'actuals': [], 'states': [], 'years': []}
                            for w in forecast_weeks}

            for test_year in test_years:
                if test_year not in actuals_by_year:
                    logger.warning(f"No actual yields for {crop} {test_year}, skipping")
                    continue

                actuals = actuals_by_year[test_year]
                train_yrs = [y for y in all_years if y != test_year and y >= 2005]

                if len(train_yrs) < 10:
                    logger.warning(f"Only {len(train_yrs)} training years for {test_year}")
                    continue

                for week in forecast_weeks:
                    # Check feature availability
                    cur.execute("""
                        SELECT COUNT(*) FROM silver.yield_features
                        WHERE crop = %s AND year = %s AND week = %s
                    """, (crop, test_year, week))
                    if cur.fetchone()[0] == 0:
                        continue

                    # Train on other years
                    metrics = self.model.train(
                        crop,
                        train_years=range(min(train_yrs), max(train_yrs) + 1),
                        target_week=week,
                    )
                    if 'error' in metrics:
                        continue

                    # Predict test year
                    predictions = self.model.predict(crop, test_year, week)
                    for p in predictions:
                        if p.state in actuals:
                            week_results[week]['predictions'].append(p.yield_forecast)
                            week_results[week]['actuals'].append(actuals[p.state])
                            week_results[week]['states'].append(p.state)
                            week_results[week]['years'].append(test_year)

                logger.info(f"  Year {test_year}: "
                            + ", ".join(f"wk{w}={len(week_results[w]['predictions'])}pts"
                                        for w in forecast_weeks
                                        if week_results[w]['predictions']))

            # Compute metrics
            results = {}
            for week in forecast_weeks:
                data = week_results[week]
                n = len(data['predictions'])
                if n == 0:
                    results[f'week_{week}'] = {'n': 0}
                    continue

                preds = np.array(data['predictions'])
                acts = np.array(data['actuals'])
                errors = preds - acts

                mean_yield = np.mean(acts)
                dir_correct = np.sum((preds > mean_yield) == (acts > mean_yield))

                ss_res = np.sum(errors ** 2)
                ss_tot = np.sum((acts - np.mean(acts)) ** 2)
                r2 = 1 - ss_res / ss_tot if ss_tot > 0 else None

                results[f'week_{week}'] = {
                    'n': n,
                    'rmse': round(float(np.sqrt(np.mean(errors ** 2))), 2),
                    'mae': round(float(np.mean(np.abs(errors))), 2),
                    'mean_error': round(float(np.mean(errors)), 2),
                    'median_error': round(float(np.median(errors)), 2),
                    'max_abs_error': round(float(np.max(np.abs(errors))), 2),
                    'r2': round(float(r2), 3) if r2 is not None else None,
                    'dir_accuracy': round(float(dir_correct / n), 3),
                    'raw': data,  # Preserved for downstream analysis
                }

            return results

        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # SKILL SCORE
    # ------------------------------------------------------------------

    def compute_skill_score(self, crop: str, test_years: list,
                            forecast_weeks: list = None) -> dict:
        """
        Compute skill score vs naive benchmarks.

        Benchmarks:
        1. Naive trend: Just use trend_yield
        2. Last year: Use previous year's actual yield
        3. 5-year average: Rolling 5-year mean

        Skill = 1 - (MSE_model / MSE_benchmark)
        Positive means model is better; negative means worse.
        """
        if forecast_weeks is None:
            forecast_weeks = BACKTEST_WEEKS

        crop_db = CROP_DB_MAP.get(crop, crop.upper())
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            # Fetch all actuals
            cur.execute("""
                SELECT year, state_abbrev, yield_per_acre
                FROM bronze.nass_state_yields
                WHERE commodity = %s AND yield_per_acre IS NOT NULL
                  AND state_abbrev IS NOT NULL
                ORDER BY year, state_abbrev
            """, (crop_db,))
            actuals_by_year = {}
            for year, state, yld in cur.fetchall():
                actuals_by_year.setdefault(year, {})[state] = float(yld)

            # Fetch trends
            cur.execute("""
                SELECT state, slope, intercept
                FROM silver.yield_trend
                WHERE commodity = %s AND trend_type = 'linear'
            """, (crop_db,))
            trends = {r[0]: (float(r[1]), float(r[2])) for r in cur.fetchall()}

            # Fetch forecasts from gold.yield_forecast (if they exist from backtest runs)
            cur.execute("""
                SELECT state, year, forecast_week, yield_forecast
                FROM gold.yield_forecast
                WHERE commodity = %s AND model_type = 'ensemble'
                  AND year = ANY(%s)
                ORDER BY year, forecast_week, state
            """, (crop_db, list(test_years)))
            forecasts = {}
            for state, year, week, yld in cur.fetchall():
                forecasts.setdefault(year, {}).setdefault(week, {})[state] = float(yld)

            results = {}
            for week in forecast_weeks:
                model_errors_sq = []
                trend_errors_sq = []
                lastyear_errors_sq = []
                avg5yr_errors_sq = []

                for test_year in test_years:
                    if test_year not in actuals_by_year:
                        continue

                    week_forecasts = forecasts.get(test_year, {}).get(week, {})
                    actuals = actuals_by_year[test_year]

                    for state, actual in actuals.items():
                        # Model prediction
                        if state in week_forecasts:
                            model_errors_sq.append((week_forecasts[state] - actual) ** 2)

                        # Trend benchmark
                        if state in trends:
                            slope, intercept = trends[state]
                            trend_pred = intercept + slope * test_year
                            trend_errors_sq.append((trend_pred - actual) ** 2)

                        # Last year benchmark
                        prev_year = test_year - 1
                        if prev_year in actuals_by_year and state in actuals_by_year[prev_year]:
                            ly_pred = actuals_by_year[prev_year][state]
                            lastyear_errors_sq.append((ly_pred - actual) ** 2)

                        # 5-year average benchmark
                        recent_yields = []
                        for y in range(test_year - 5, test_year):
                            if y in actuals_by_year and state in actuals_by_year[y]:
                                recent_yields.append(actuals_by_year[y][state])
                        if len(recent_yields) >= 3:
                            avg5yr_pred = np.mean(recent_yields)
                            avg5yr_errors_sq.append((avg5yr_pred - actual) ** 2)

                week_result = {'n_model': len(model_errors_sq)}

                if model_errors_sq:
                    mse_model = np.mean(model_errors_sq)
                    week_result['mse_model'] = round(float(mse_model), 2)
                    week_result['rmse_model'] = round(float(np.sqrt(mse_model)), 2)

                    if trend_errors_sq:
                        mse_trend = np.mean(trend_errors_sq)
                        week_result['skill_vs_trend'] = round(1 - mse_model / mse_trend, 3) if mse_trend > 0 else None

                    if lastyear_errors_sq:
                        mse_ly = np.mean(lastyear_errors_sq)
                        week_result['skill_vs_last_year'] = round(1 - mse_model / mse_ly, 3) if mse_ly > 0 else None

                    if avg5yr_errors_sq:
                        mse_5yr = np.mean(avg5yr_errors_sq)
                        week_result['skill_vs_5yr_avg'] = round(1 - mse_model / mse_5yr, 3) if mse_5yr > 0 else None

                results[f'week_{week}'] = week_result

            return results

        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # BIAS ANALYSIS
    # ------------------------------------------------------------------

    def analyze_bias(self, crop: str, test_years: list) -> dict:
        """
        Analyze systematic bias in forecasts.

        Checks:
        1. Overall bias (mean error)
        2. State-level bias
        3. Season-phase bias
        4. Extreme year bias
        """
        crop_db = CROP_DB_MAP.get(crop, crop.upper())
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            # Pull all forecasts vs actuals
            cur.execute("""
                SELECT f.state, f.year, f.forecast_week, f.yield_forecast,
                       a.yield_per_acre as actual
                FROM gold.yield_forecast f
                JOIN bronze.nass_state_yields a
                    ON f.state = a.state_abbrev
                    AND f.year = a.year
                    AND a.commodity = %s
                WHERE f.commodity = %s
                  AND f.model_type = 'ensemble'
                  AND f.year = ANY(%s)
                  AND a.yield_per_acre IS NOT NULL
                ORDER BY f.year, f.forecast_week, f.state
            """, (crop_db, crop_db, list(test_years)))
            rows = cur.fetchall()

            if not rows:
                return {'error': 'No forecast/actual pairs found'}

            # Organize data
            records = []
            for state, year, week, forecast, actual in rows:
                records.append({
                    'state': state,
                    'year': year,
                    'week': week,
                    'forecast': float(forecast),
                    'actual': float(actual),
                    'error': float(forecast) - float(actual),
                })

            errors = np.array([r['error'] for r in records])
            report = {}

            # 1. Overall bias
            report['overall'] = {
                'n': len(records),
                'mean_error': round(float(np.mean(errors)), 2),
                'std_error': round(float(np.std(errors)), 2),
                'median_error': round(float(np.median(errors)), 2),
                'pct_over': round(float(np.mean(errors > 0)) * 100, 1),
                'pct_under': round(float(np.mean(errors < 0)) * 100, 1),
            }

            # 2. State-level bias
            state_bias = {}
            for r in records:
                state_bias.setdefault(r['state'], []).append(r['error'])

            state_results = {}
            for state, errs in state_bias.items():
                errs_arr = np.array(errs)
                state_results[state] = {
                    'n': len(errs),
                    'mean_error': round(float(np.mean(errs_arr)), 2),
                    'rmse': round(float(np.sqrt(np.mean(errs_arr ** 2))), 2),
                }

            # Sort by absolute mean error
            worst_states = sorted(state_results.items(),
                                  key=lambda x: abs(x[1]['mean_error']), reverse=True)
            report['worst_states'] = dict(worst_states[:10])

            # 3. Season-phase bias (by forecast week)
            week_bias = {}
            for r in records:
                week_bias.setdefault(r['week'], []).append(r['error'])

            report['by_week'] = {}
            for week in sorted(week_bias.keys()):
                errs_arr = np.array(week_bias[week])
                report['by_week'][f'week_{week}'] = {
                    'n': len(week_bias[week]),
                    'mean_error': round(float(np.mean(errs_arr)), 2),
                    'rmse': round(float(np.sqrt(np.mean(errs_arr ** 2))), 2),
                }

            # 4. Extreme year detection
            year_errors = {}
            for r in records:
                year_errors.setdefault(r['year'], []).append(r['error'])

            report['by_year'] = {}
            for year in sorted(year_errors.keys()):
                errs_arr = np.array(year_errors[year])
                report['by_year'][str(year)] = {
                    'n': len(year_errors[year]),
                    'mean_error': round(float(np.mean(errs_arr)), 2),
                    'rmse': round(float(np.sqrt(np.mean(errs_arr ** 2))), 2),
                }

            return report

        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # REVISION TRACKING
    # ------------------------------------------------------------------

    def track_revisions(self, crop: str, year: int) -> list:
        """
        Query gold.yield_forecast for week-over-week forecast changes.

        Returns list of revision events.
        """
        crop_db = CROP_DB_MAP.get(crop, crop.upper())
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            cur.execute("""
                SELECT state, forecast_week, yield_forecast,
                       prev_week_forecast, wow_change, primary_driver
                FROM gold.yield_forecast
                WHERE commodity = %s AND year = %s AND model_type = 'ensemble'
                  AND wow_change IS NOT NULL
                ORDER BY ABS(wow_change) DESC
                LIMIT 50
            """, (crop_db, year))

            revisions = []
            for state, week, forecast, prev, change, driver in cur.fetchall():
                revisions.append({
                    'state': state,
                    'week': week,
                    'forecast': float(forecast),
                    'previous': float(prev) if prev else None,
                    'change': float(change),
                    'driver': driver,
                })

            return revisions

        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # VALIDATION REPORT
    # ------------------------------------------------------------------

    def generate_validation_report(self, crop: str, test_years: list,
                                   output_path: str = None) -> str:
        """Generate comprehensive validation report."""
        lines = []
        lines.append(f"# Yield Model Validation Report — {crop.replace('_', ' ').title()}\n")
        lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        lines.append(f"**Test Years:** {min(test_years)}–{max(test_years)}\n")

        # Section 1: Backtest accuracy
        lines.append("## 1. Accuracy by Forecast Week\n")
        bt_results = self.run_backtest(crop, test_years)

        lines.append("| Week | N | RMSE | MAE | Mean Error | Dir Accuracy | Target RMSE | Pass? |")
        lines.append("|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|")

        for week in BACKTEST_WEEKS:
            key = f'week_{week}'
            m = bt_results.get(key, {})
            n = m.get('n', 0)
            if n == 0:
                lines.append(f"| {week} | 0 | — | — | — | — | — | — |")
                continue

            target = ACCURACY_TARGETS_CORN.get(week, '—')
            passed = "Yes" if isinstance(target, (int, float)) and m['rmse'] <= target else "No"
            lines.append(
                f"| {week} | {n} | {m['rmse']:.1f} | {m['mae']:.1f} | "
                f"{m['mean_error']:+.1f} | {m['dir_accuracy']:.0%} | "
                f"{target} | {passed} |"
            )

        # Section 2: Skill scores
        lines.append("\n## 2. Skill Scores vs Benchmarks\n")
        skill_results = self.compute_skill_score(crop, test_years)

        lines.append("| Week | Skill vs Trend | Skill vs Last Year | Skill vs 5yr Avg |")
        lines.append("|:---:|:---:|:---:|:---:|")

        for week in BACKTEST_WEEKS:
            key = f'week_{week}'
            s = skill_results.get(key, {})
            if s.get('n_model', 0) == 0:
                lines.append(f"| {week} | — | — | — |")
                continue

            vs_trend = f"{s.get('skill_vs_trend', 0):+.3f}" if s.get('skill_vs_trend') is not None else "—"
            vs_ly = f"{s.get('skill_vs_last_year', 0):+.3f}" if s.get('skill_vs_last_year') is not None else "—"
            vs_5yr = f"{s.get('skill_vs_5yr_avg', 0):+.3f}" if s.get('skill_vs_5yr_avg') is not None else "—"
            lines.append(f"| {week} | {vs_trend} | {vs_ly} | {vs_5yr} |")

        lines.append("\n> Positive skill = model outperforms benchmark. "
                      "Target: > 0.200 by week 26.\n")

        # Section 3: Bias analysis
        lines.append("## 3. Bias Analysis\n")
        bias = self.analyze_bias(crop, test_years)

        if 'error' not in bias:
            overall = bias.get('overall', {})
            lines.append(f"- **Overall mean error:** {overall.get('mean_error', 'N/A')} bu/ac")
            lines.append(f"- **Overpredict %:** {overall.get('pct_over', 'N/A')}%")
            lines.append(f"- **Underpredict %:** {overall.get('pct_under', 'N/A')}%\n")

            # Worst states
            worst = bias.get('worst_states', {})
            if worst:
                lines.append("### Hardest States to Predict\n")
                lines.append("| State | Mean Error | RMSE | N |")
                lines.append("|:---:|:---:|:---:|:---:|")
                for state, info in list(worst.items())[:5]:
                    lines.append(f"| {state} | {info['mean_error']:+.1f} | {info['rmse']:.1f} | {info['n']} |")

            # By year
            by_year = bias.get('by_year', {})
            if by_year:
                lines.append("\n### Accuracy by Year\n")
                lines.append("| Year | Mean Error | RMSE | N |")
                lines.append("|:---:|:---:|:---:|:---:|")
                for year, info in sorted(by_year.items()):
                    lines.append(f"| {year} | {info['mean_error']:+.1f} | {info['rmse']:.1f} | {info['n']} |")
        else:
            lines.append(f"No forecast/actual data available for bias analysis.\n")

        # Section 4: Worst cases
        lines.append("\n## 4. Worst-Case Analysis\n")
        for week in [26, 30]:
            key = f'week_{week}'
            data = bt_results.get(key, {})
            raw = data.get('raw', {})
            if not raw or not raw.get('predictions'):
                continue

            preds = np.array(raw['predictions'])
            acts = np.array(raw['actuals'])
            errors = preds - acts
            states = raw['states']
            years = raw['years']

            # Top 5 worst errors
            worst_idx = np.argsort(np.abs(errors))[-5:][::-1]
            lines.append(f"### Week {week} — Largest Errors\n")
            lines.append("| Year | State | Predicted | Actual | Error |")
            lines.append("|:---:|:---:|:---:|:---:|:---:|")
            for i in worst_idx:
                lines.append(f"| {years[i]} | {states[i]} | {preds[i]:.1f} | {acts[i]:.1f} | {errors[i]:+.1f} |")

        # Section 5: Recommendations
        lines.append("\n## 5. Recommendations\n")
        overall_bias = bias.get('overall', {}).get('mean_error', 0)
        if abs(overall_bias) > 2:
            direction = "high" if overall_bias > 0 else "low"
            lines.append(f"- **Bias correction needed:** Model consistently predicts {direction} "
                          f"by {abs(overall_bias):.1f} bu/ac on average")
        else:
            lines.append("- **Bias:** Within acceptable range")

        # Check if accuracy degrades for specific weeks
        for week in [26, 30]:
            key = f'week_{week}'
            data = bt_results.get(key, {})
            target = ACCURACY_TARGETS_CORN.get(week)
            if data.get('rmse') and target and data['rmse'] > target:
                lines.append(f"- **Week {week}:** RMSE {data['rmse']:.1f} exceeds target {target}. "
                             "Consider adding more features or adjusting ensemble weights.")

        skill_26 = skill_results.get('week_26', {}).get('skill_vs_trend')
        if skill_26 is not None and skill_26 < 0.2:
            lines.append(f"- **Skill score at week 26:** {skill_26:.3f} is below 0.200 target. "
                         "Model adds limited value over naive trend at this point.")

        lines.append("")

        output = "\n".join(lines)

        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(output)
            logger.info(f"Validation report saved to {output_path}")

        return output


def main():
    parser = argparse.ArgumentParser(description="Yield Model Validator")
    subparsers = parser.add_subparsers(dest='command')

    # Backtest
    bt_parser = subparsers.add_parser('backtest', help='Run backtesting')
    bt_parser.add_argument('--crop', type=str, required=True)
    bt_parser.add_argument('--years', type=str, default='2020-2024')

    # Skill scores
    skill_parser = subparsers.add_parser('skill', help='Compute skill scores')
    skill_parser.add_argument('--crop', type=str, required=True)
    skill_parser.add_argument('--years', type=str, default='2020-2024')

    # Bias analysis
    bias_parser = subparsers.add_parser('bias', help='Analyze bias')
    bias_parser.add_argument('--crop', type=str, required=True)
    bias_parser.add_argument('--years', type=str, default='2020-2024')

    # Revision tracking
    rev_parser = subparsers.add_parser('revisions', help='Track revisions')
    rev_parser.add_argument('--crop', type=str, required=True)
    rev_parser.add_argument('--year', type=int, default=date.today().year)

    # Full report
    report_parser = subparsers.add_parser('report', help='Generate validation report')
    report_parser.add_argument('--crop', type=str, required=True)
    report_parser.add_argument('--years', type=str, default='2020-2024')
    report_parser.add_argument('--output', type=str, help='Output file path')

    # Add --verbose to every subparser
    for sp in [bt_parser, skill_parser, bias_parser, rev_parser, report_parser]:
        sp.add_argument('--verbose', '-v', action='store_true')
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    validator = YieldModelValidator()

    if args.command == 'backtest':
        start, end = map(int, args.years.split('-'))
        results = validator.run_backtest(args.crop, list(range(start, end + 1)))
        print(f"\nBacktest Results — {args.crop}:")
        print(f"{'Week':>6} {'N':>5} {'RMSE':>7} {'MAE':>7} {'Bias':>7} {'Dir Acc':>8}")
        print(f"{'-'*6} {'-'*5} {'-'*7} {'-'*7} {'-'*7} {'-'*8}")
        for key in sorted(results.keys()):
            m = results[key]
            if m.get('n', 0) > 0:
                print(f"{key:>6} {m['n']:>5} {m['rmse']:>7.2f} {m['mae']:>7.2f} "
                      f"{m['mean_error']:>7.2f} {m['dir_accuracy']:>7.1%}")

    elif args.command == 'skill':
        start, end = map(int, args.years.split('-'))
        results = validator.compute_skill_score(args.crop, list(range(start, end + 1)))
        print(f"\nSkill Scores — {args.crop}:")
        print(f"{'Week':>6} {'vs Trend':>10} {'vs Last Yr':>11} {'vs 5yr Avg':>11}")
        print(f"{'-'*6} {'-'*10} {'-'*11} {'-'*11}")
        for key in sorted(results.keys()):
            s = results[key]
            if s.get('n_model', 0) > 0:
                vt = f"{s.get('skill_vs_trend', 0):+.3f}" if s.get('skill_vs_trend') is not None else "N/A"
                vl = f"{s.get('skill_vs_last_year', 0):+.3f}" if s.get('skill_vs_last_year') is not None else "N/A"
                va = f"{s.get('skill_vs_5yr_avg', 0):+.3f}" if s.get('skill_vs_5yr_avg') is not None else "N/A"
                print(f"{key:>6} {vt:>10} {vl:>11} {va:>11}")

    elif args.command == 'bias':
        start, end = map(int, args.years.split('-'))
        results = validator.analyze_bias(args.crop, list(range(start, end + 1)))
        if 'error' in results:
            print(f"No data: {results['error']}")
        else:
            print(f"\nBias Analysis — {args.crop}:")
            overall = results.get('overall', {})
            print(f"  Overall mean error: {overall.get('mean_error', 'N/A')} bu/ac")
            print(f"  Overpredict: {overall.get('pct_over', 'N/A')}%, "
                  f"Underpredict: {overall.get('pct_under', 'N/A')}%")

            worst = results.get('worst_states', {})
            if worst:
                print(f"\n  Hardest states:")
                for state, info in list(worst.items())[:5]:
                    print(f"    {state}: bias={info['mean_error']:+.1f}, RMSE={info['rmse']:.1f}")

    elif args.command == 'revisions':
        revisions = validator.track_revisions(args.crop, args.year)
        if revisions:
            print(f"\nLargest Forecast Revisions — {args.crop} {args.year}:")
            print(f"{'State':>6} {'Week':>5} {'Forecast':>9} {'Previous':>9} {'Change':>7} {'Driver'}")
            print(f"{'-'*6} {'-'*5} {'-'*9} {'-'*9} {'-'*7} {'-'*20}")
            for r in revisions[:20]:
                prev = f"{r['previous']:.1f}" if r['previous'] else "—"
                print(f"{r['state']:>6} {r['week']:>5} {r['forecast']:>9.1f} "
                      f"{prev:>9} {r['change']:>+7.1f} {r['driver'] or ''}")
        else:
            print("No revisions found.")

    elif args.command == 'report':
        start, end = map(int, args.years.split('-'))
        output = validator.generate_validation_report(
            args.crop,
            list(range(start, end + 1)),
            output_path=args.output,
        )
        print(output)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
