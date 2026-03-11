"""
Forecast Tracker — PostgreSQL-backed

Captures forecasts, matches to actuals, computes accuracy metrics.
Ported from scripts/deployment/forecast_tracker.py (SQLite) to use
the project's PostgreSQL database via get_connection().

Key features:
- Record forecasts with vintage tracking
- Record actual values with revision support
- Auto-match forecasts to actuals and compute errors
- Compute accuracy metrics (MAE, MAPE, RMSE, Theil's U, directional accuracy)
- Analyze bias and suggest improvements
"""

import json
import logging
import math
from dataclasses import dataclass, asdict
from datetime import datetime, date
from enum import Enum
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class ForecastType(Enum):
    PRICE = "price"
    PRODUCTION = "production"
    ENDING_STOCKS = "ending_stocks"
    EXPORTS = "exports"
    CRUSH = "crush"
    YIELD = "yield"
    AREA = "area"
    CONSUMPTION = "consumption"
    IMPORTS = "imports"
    CROP_CONDITION = "crop_condition"


@dataclass
class Forecast:
    forecast_id: str
    forecast_date: str
    target_date: str
    commodity: str
    country: str
    forecast_type: str
    value: float
    unit: str
    confidence_low: float = None
    confidence_high: float = None
    marketing_year: str = None
    notes: str = None
    source: str = "RLC"
    analyst: str = None


@dataclass
class Actual:
    actual_id: str
    report_date: str
    target_date: str
    commodity: str
    country: str
    value_type: str
    value: float
    unit: str
    marketing_year: str = None
    source: str = None
    revision_number: int = 0
    notes: str = None


@dataclass
class AccuracyMetrics:
    metric_id: str
    computed_date: str
    commodity: str
    country: str
    forecast_type: str
    horizon: str
    n_forecasts: int
    mae: float
    mape: float
    rmse: float
    mpe: float
    directional_accuracy: float
    mean_error: float
    median_error: float
    min_error: float
    max_error: float
    std_error: float
    theil_u: float = None
    period_start: str = None
    period_end: str = None


def _get_connection():
    from src.services.database.db_config import get_connection
    return get_connection()


class ForecastTracker:
    """PostgreSQL-backed forecast tracking and accuracy measurement."""

    @staticmethod
    def generate_forecast_id(forecast: Forecast) -> str:
        components = [
            forecast.forecast_date, forecast.target_date,
            forecast.commodity, forecast.country, forecast.forecast_type
        ]
        return "_".join(str(c).replace("/", "-").replace(" ", "_") for c in components)

    @staticmethod
    def generate_actual_id(actual: Actual) -> str:
        components = [
            actual.report_date, actual.target_date,
            actual.commodity, actual.country, actual.value_type,
            actual.source or "unknown", actual.revision_number
        ]
        return "_".join(str(c).replace("/", "-").replace(" ", "_") for c in components)

    def record_forecast(self, forecast: Forecast) -> str:
        """Record a new forecast. Returns forecast_id."""
        if not forecast.forecast_id:
            forecast.forecast_id = self.generate_forecast_id(forecast)

        with _get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO core.forecasts
                    (forecast_id, forecast_date, target_date, commodity, country,
                     forecast_type, value, unit, confidence_low, confidence_high,
                     marketing_year, notes, source, analyst)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (forecast_id) DO UPDATE SET
                    value = EXCLUDED.value,
                    notes = EXCLUDED.notes,
                    created_at = NOW()
            """, (
                forecast.forecast_id, forecast.forecast_date, forecast.target_date,
                forecast.commodity, forecast.country, forecast.forecast_type,
                forecast.value, forecast.unit, forecast.confidence_low,
                forecast.confidence_high, forecast.marketing_year,
                forecast.notes, forecast.source, forecast.analyst
            ))
            conn.commit()

        logger.info(f"Recorded forecast: {forecast.forecast_id}")
        return forecast.forecast_id

    def record_actual(self, actual: Actual) -> str:
        """Record an actual value. Returns actual_id."""
        if not actual.actual_id:
            actual.actual_id = self.generate_actual_id(actual)

        with _get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO core.actuals
                    (actual_id, report_date, target_date, commodity, country,
                     value_type, value, unit, marketing_year, source,
                     revision_number, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (target_date, commodity, country, value_type, source, revision_number)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    notes = EXCLUDED.notes,
                    created_at = NOW()
            """, (
                actual.actual_id, actual.report_date, actual.target_date,
                actual.commodity, actual.country, actual.value_type,
                actual.value, actual.unit, actual.marketing_year,
                actual.source, actual.revision_number, actual.notes
            ))
            conn.commit()

        logger.info(f"Recorded actual: {actual.actual_id}")
        return actual.actual_id

    def match_forecasts_to_actuals(self) -> int:
        """Match unmatched forecasts to actuals and compute errors. Returns pair count."""
        with _get_connection() as conn:
            cur = conn.cursor()

            # Find unmatched pairs
            cur.execute("""
                SELECT
                    f.forecast_id, f.forecast_date, f.target_date,
                    f.commodity, f.country, f.forecast_type,
                    f.value AS forecast_value,
                    a.actual_id, a.report_date,
                    a.value AS actual_value
                FROM core.forecasts f
                JOIN core.actuals a ON
                    f.target_date = a.target_date AND
                    f.commodity = a.commodity AND
                    f.country = a.country AND
                    f.forecast_type = a.value_type
                WHERE NOT EXISTS (
                    SELECT 1 FROM core.forecast_actual_pairs p
                    WHERE p.forecast_id = f.forecast_id AND p.actual_id = a.actual_id
                )
            """)
            pairs = cur.fetchall()

            for pair in pairs:
                forecast_id = pair['forecast_id']
                forecast_date = pair['forecast_date']
                target_date = pair['target_date']
                commodity = pair['commodity']
                country = pair['country']
                forecast_type = pair['forecast_type']
                forecast_value = float(pair['forecast_value'])
                actual_id = pair['actual_id']
                report_date = pair['report_date']
                actual_value = float(pair['actual_value'])
                error = actual_value - forecast_value
                abs_error = abs(error)
                pct_error = (error / actual_value * 100) if actual_value != 0 else None
                abs_pct_error = abs(pct_error) if pct_error is not None else None

                # Days ahead
                try:
                    forecast_dt = datetime.strptime(str(forecast_date), "%Y-%m-%d")
                    target_dt = datetime.strptime(str(target_date), "%Y-%m-%d")
                    days_ahead = (target_dt - forecast_dt).days
                except Exception:
                    days_ahead = None

                # Direction correctness
                cur.execute("""
                    SELECT value FROM core.actuals
                    WHERE commodity = %s AND country = %s AND value_type = %s
                        AND target_date < %s
                    ORDER BY target_date DESC LIMIT 1
                """, (commodity, country, forecast_type, target_date))
                prev_row = cur.fetchone()

                direction_correct = None
                if prev_row:
                    prev_value = float(prev_row['value'])
                    actual_dir = 1 if actual_value > prev_value else (-1 if actual_value < prev_value else 0)
                    forecast_dir = 1 if forecast_value > prev_value else (-1 if forecast_value < prev_value else 0)
                    direction_correct = 1 if actual_dir == forecast_dir else 0

                cur.execute("""
                    INSERT INTO core.forecast_actual_pairs
                        (forecast_id, actual_id, error, percentage_error,
                         absolute_error, absolute_percentage_error,
                         direction_correct, days_ahead)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (forecast_id, actual_id) DO NOTHING
                """, (
                    forecast_id, actual_id, error, pct_error,
                    abs_error, abs_pct_error, direction_correct, days_ahead
                ))

            conn.commit()

        logger.info(f"Matched {len(pairs)} forecast-actual pairs")
        return len(pairs)

    def compute_accuracy_metrics(self, commodity: str = None, country: str = None,
                                  forecast_type: str = None,
                                  period_start: str = None,
                                  period_end: str = None) -> Optional[AccuracyMetrics]:
        """Compute accuracy metrics for a set of forecasts."""
        query = """
            SELECT
                p.error, p.percentage_error, p.absolute_error,
                p.absolute_percentage_error, p.direction_correct, p.days_ahead,
                f.value AS forecast_value, a.value AS actual_value,
                f.commodity, f.country, f.forecast_type
            FROM core.forecast_actual_pairs p
            JOIN core.forecasts f ON p.forecast_id = f.forecast_id
            JOIN core.actuals a ON p.actual_id = a.actual_id
            WHERE 1=1
        """
        params = []

        if commodity:
            query += " AND f.commodity = %s"
            params.append(commodity)
        if country:
            query += " AND f.country = %s"
            params.append(country)
        if forecast_type:
            query += " AND f.forecast_type = %s"
            params.append(forecast_type)
        if period_start:
            query += " AND f.target_date >= %s"
            params.append(period_start)
        if period_end:
            query += " AND f.target_date <= %s"
            params.append(period_end)

        with _get_connection() as conn:
            df = pd.read_sql(query, conn, params=params)

        if len(df) == 0:
            logger.info("No forecast-actual pairs found")
            return None

        n = len(df)
        errors = df['error'].dropna()
        pct_errors = df['percentage_error'].dropna()
        abs_errors = df['absolute_error'].dropna()
        abs_pct_errors = df['absolute_percentage_error'].dropna()

        mae = float(abs_errors.mean()) if len(abs_errors) > 0 else None
        mape = float(abs_pct_errors.mean()) if len(abs_pct_errors) > 0 else None
        rmse = math.sqrt(float((errors ** 2).mean())) if len(errors) > 0 else None
        mpe = float(pct_errors.mean()) if len(pct_errors) > 0 else None

        direction_vals = df['direction_correct'].dropna()
        directional_accuracy = float(direction_vals.mean() * 100) if len(direction_vals) > 0 else None

        mean_error = float(errors.mean()) if len(errors) > 0 else None
        median_error = float(errors.median()) if len(errors) > 0 else None
        min_error = float(errors.min()) if len(errors) > 0 else None
        max_error = float(errors.max()) if len(errors) > 0 else None
        std_error = float(errors.std()) if len(errors) > 0 else None

        # Theil's U statistic
        theil_u = None
        if len(df) > 1:
            forecast_sq = (df['actual_value'] - df['forecast_value']) ** 2
            naive_sq = df['actual_value'].diff() ** 2
            if naive_sq.sum() > 0:
                theil_u = math.sqrt(float(forecast_sq.mean() / naive_sq.mean()))

        metric_id = f"metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        metrics = AccuracyMetrics(
            metric_id=metric_id,
            computed_date=datetime.now().strftime("%Y-%m-%d"),
            commodity=commodity or "all",
            country=country or "all",
            forecast_type=forecast_type or "all",
            horizon="all",
            n_forecasts=n,
            mae=mae, mape=mape, rmse=rmse, mpe=mpe,
            directional_accuracy=directional_accuracy,
            mean_error=mean_error, median_error=median_error,
            min_error=min_error, max_error=max_error,
            std_error=std_error, theil_u=theil_u,
            period_start=period_start, period_end=period_end,
        )

        self._save_metrics(metrics)
        return metrics

    def _save_metrics(self, metrics: AccuracyMetrics):
        with _get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO core.accuracy_metrics
                    (metric_id, computed_date, commodity, country, forecast_type,
                     horizon, n_forecasts, mae, mape, rmse, mpe,
                     directional_accuracy, mean_error, median_error,
                     min_error, max_error, std_error, theil_u,
                     period_start, period_end)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                metrics.metric_id, metrics.computed_date, metrics.commodity,
                metrics.country, metrics.forecast_type, metrics.horizon,
                metrics.n_forecasts, metrics.mae, metrics.mape, metrics.rmse,
                metrics.mpe, metrics.directional_accuracy, metrics.mean_error,
                metrics.median_error, metrics.min_error, metrics.max_error,
                metrics.std_error, metrics.theil_u,
                metrics.period_start, metrics.period_end,
            ))
            conn.commit()

    def analyze_bias(self, commodity: str = None) -> pd.DataFrame:
        """Analyze systematic bias in forecasts."""
        query = """
            SELECT
                f.commodity, f.country, f.forecast_type,
                COUNT(*) AS n_forecasts,
                AVG(p.error) AS mean_error,
                AVG(p.percentage_error) AS mean_pct_error,
                CASE
                    WHEN AVG(p.percentage_error) > 2 THEN 'Over-forecasting'
                    WHEN AVG(p.percentage_error) < -2 THEN 'Under-forecasting'
                    ELSE 'Relatively unbiased'
                END AS bias_direction,
                STDDEV(p.percentage_error) AS pct_error_std,
                MIN(p.percentage_error) AS min_pct_error,
                MAX(p.percentage_error) AS max_pct_error
            FROM core.forecast_actual_pairs p
            JOIN core.forecasts f ON p.forecast_id = f.forecast_id
            WHERE p.percentage_error IS NOT NULL
        """
        if commodity:
            query += f" AND f.commodity = '{commodity}'"
        query += """
            GROUP BY f.commodity, f.country, f.forecast_type
            HAVING COUNT(*) >= 3
            ORDER BY ABS(AVG(p.percentage_error)) DESC
        """
        with _get_connection() as conn:
            return pd.read_sql(query, conn)

    def generate_accuracy_report(self, output_format: str = "markdown") -> str:
        """Generate a comprehensive accuracy report."""
        with _get_connection() as conn:
            overall = pd.read_sql("""
                SELECT COUNT(*) AS total_forecasts,
                       AVG(absolute_percentage_error) AS overall_mape,
                       AVG(CASE WHEN direction_correct = 1 THEN 100 ELSE 0 END) AS directional_accuracy
                FROM core.forecast_actual_pairs
                WHERE absolute_percentage_error IS NOT NULL
            """, conn)

            by_commodity = pd.read_sql("""
                SELECT f.commodity, COUNT(*) AS n_forecasts,
                       AVG(p.absolute_percentage_error) AS mape,
                       AVG(CASE WHEN p.direction_correct = 1 THEN 100 ELSE 0 END) AS directional_accuracy,
                       AVG(p.percentage_error) AS bias
                FROM core.forecast_actual_pairs p
                JOIN core.forecasts f ON p.forecast_id = f.forecast_id
                GROUP BY f.commodity ORDER BY COUNT(*) DESC
            """, conn)

            by_type = pd.read_sql("""
                SELECT f.forecast_type, COUNT(*) AS n_forecasts,
                       AVG(p.absolute_percentage_error) AS mape,
                       AVG(CASE WHEN p.direction_correct = 1 THEN 100 ELSE 0 END) AS directional_accuracy
                FROM core.forecast_actual_pairs p
                JOIN core.forecasts f ON p.forecast_id = f.forecast_id
                GROUP BY f.forecast_type ORDER BY COUNT(*) DESC
            """, conn)

            trend = pd.read_sql("""
                SELECT to_char(f.target_date, 'YYYY-MM') AS month,
                       COUNT(*) AS n_forecasts,
                       AVG(p.absolute_percentage_error) AS mape
                FROM core.forecast_actual_pairs p
                JOIN core.forecasts f ON p.forecast_id = f.forecast_id
                WHERE f.target_date >= CURRENT_DATE - INTERVAL '12 months'
                GROUP BY to_char(f.target_date, 'YYYY-MM')
                ORDER BY month
            """, conn)

        if output_format == "json":
            return json.dumps({
                "overall": overall.to_dict(orient='records'),
                "by_commodity": by_commodity.to_dict(orient='records'),
                "by_type": by_type.to_dict(orient='records'),
                "trend": trend.to_dict(orient='records'),
            }, indent=2)

        # Markdown report
        total = int(overall['total_forecasts'].iloc[0]) if len(overall) > 0 else 0
        omape = overall['overall_mape'].iloc[0] if total > 0 else 0
        odir = overall['directional_accuracy'].iloc[0] if total > 0 else 0

        lines = [
            f"# RLC Forecast Accuracy Report",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n",
            f"## Executive Summary\n",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Total Forecasts Evaluated | {total:,} |",
            f"| Overall MAPE | {omape:.2f}% |",
            f"| Directional Accuracy | {odir:.1f}% |\n",
            f"## Performance by Commodity\n",
            f"| Commodity | N | MAPE | Dir. Accuracy | Bias |",
            f"|-----------|---|------|---------------|------|",
        ]
        for _, r in by_commodity.iterrows():
            bias = "Over" if r['bias'] > 0 else "Under" if r['bias'] < 0 else "Neutral"
            lines.append(
                f"| {r['commodity']} | {int(r['n_forecasts'])} | "
                f"{r['mape']:.2f}% | {r['directional_accuracy']:.1f}% | "
                f"{bias} ({r['bias']:.1f}%) |"
            )

        lines += ["", "## Performance by Type\n",
                   "| Type | N | MAPE | Dir. Accuracy |",
                   "|------|---|------|---------------|"]
        for _, r in by_type.iterrows():
            lines.append(
                f"| {r['forecast_type']} | {int(r['n_forecasts'])} | "
                f"{r['mape']:.2f}% | {r['directional_accuracy']:.1f}% |"
            )

        lines += ["", "## 12-Month Trend\n",
                   "| Month | N | MAPE |", "|-------|---|------|"]
        for _, r in trend.iterrows():
            lines.append(f"| {r['month']} | {int(r['n_forecasts'])} | {r['mape']:.2f}% |")

        return "\n".join(lines)

    def get_improvement_suggestions(self) -> List[Dict]:
        """Analyze patterns and suggest improvements."""
        bias_df = self.analyze_bias()
        suggestions = []
        for _, row in bias_df.iterrows():
            if row['bias_direction'] != 'Relatively unbiased':
                suggestions.append({
                    "commodity": row['commodity'],
                    "country": row['country'],
                    "forecast_type": row['forecast_type'],
                    "issue": row['bias_direction'],
                    "mean_bias": f"{row['mean_pct_error']:.2f}%",
                    "suggestion": _generate_suggestion(row),
                })
        return suggestions


def _generate_suggestion(row) -> str:
    bias = row['mean_pct_error']
    ft = row['forecast_type']
    if bias > 5:
        if ft == 'production':
            return "Consider applying a yield discount factor or reviewing planted area assumptions"
        elif ft == 'price':
            return "Review demand assumptions; may be overestimating demand strength"
        elif ft == 'exports':
            return "Consider logistics constraints and competitor supply more carefully"
        return f"Apply a {abs(bias):.1f}% downward adjustment to initial estimates"
    elif bias < -5:
        if ft == 'production':
            return "May be too conservative on yields; review technology/weather assumptions"
        elif ft == 'price':
            return "Review supply assumptions; may be overestimating supply availability"
        elif ft == 'exports':
            return "May be underestimating demand; review importing country needs"
        return f"Apply a {abs(bias):.1f}% upward adjustment to initial estimates"
    return "No significant bias detected"
