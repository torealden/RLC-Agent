#!/usr/bin/env python3
"""
RLC Forecast Tracking System

A comprehensive system for capturing forecasts, comparing to actuals,
measuring accuracy, and creating feedback loops for continuous improvement.

Key Features:
- Capture price and fundamental forecasts with vintage tracking
- Record actual values when reported
- Compute accuracy metrics (MAPE, RMSE, MAE, Directional Accuracy)
- Analyze forecast bias and systematic errors
- Generate accuracy reports for marketing and improvement
- Provide feedback for methodology refinement

Usage:
    python deployment/forecast_tracker.py --record-forecast
    python deployment/forecast_tracker.py --record-actual
    python deployment/forecast_tracker.py --compute-accuracy
    python deployment/forecast_tracker.py --report
    python deployment/forecast_tracker.py --analyze-bias
    python deployment/forecast_tracker.py --export-for-powerbi
"""

import argparse
import sqlite3
import json
import math
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

import pandas as pd

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "rlc_commodities.db"
REPORTS_DIR = DATA_DIR / "forecast_reports"


class ForecastType(Enum):
    """Types of forecasts we track."""
    PRICE = "price"                    # Price forecasts (e.g., soybean price $12.50/bu)
    PRODUCTION = "production"          # Production forecasts (e.g., 4,500 MMT)
    ENDING_STOCKS = "ending_stocks"    # Ending stocks forecasts
    EXPORTS = "exports"                # Export forecasts
    CRUSH = "crush"                    # Crush/processing forecasts
    YIELD = "yield"                    # Yield forecasts (bu/acre, MT/ha)
    AREA = "area"                      # Planted/harvested area
    CONSUMPTION = "consumption"        # Domestic consumption
    IMPORTS = "imports"                # Import forecasts


class TimeHorizon(Enum):
    """Standard forecast horizons."""
    CURRENT_MONTH = "current_month"
    NEXT_MONTH = "1_month"
    NEXT_QUARTER = "3_month"
    NEXT_HALF_YEAR = "6_month"
    NEXT_YEAR = "12_month"
    MARKETING_YEAR = "marketing_year"


@dataclass
class Forecast:
    """A single forecast record."""
    forecast_id: str              # Unique ID
    forecast_date: str            # When the forecast was made (vintage)
    target_date: str              # What date/period is being forecasted
    commodity: str                # e.g., "soybeans", "corn"
    country: str                  # e.g., "US", "Brazil", "World"
    forecast_type: str            # From ForecastType enum
    value: float                  # The forecasted value
    unit: str                     # e.g., "USD/bushel", "MMT", "1000 hectares"
    confidence_low: float = None  # Lower confidence bound (optional)
    confidence_high: float = None # Upper confidence bound (optional)
    marketing_year: str = None    # e.g., "2023/24"
    notes: str = None             # Optional commentary
    source: str = "RLC"           # Source of forecast
    analyst: str = None           # Who made the forecast


@dataclass
class Actual:
    """An actual reported value."""
    actual_id: str
    report_date: str              # When the actual was reported
    target_date: str              # What period this actual represents
    commodity: str
    country: str
    value_type: str               # Same as forecast_type
    value: float
    unit: str
    marketing_year: str = None
    source: str = None            # e.g., "USDA", "CONAB", "Statistics Canada"
    revision_number: int = 0      # Track revisions (0 = initial, 1 = first revision, etc.)
    notes: str = None


@dataclass
class AccuracyMetrics:
    """Computed accuracy metrics for a set of forecasts."""
    metric_id: str
    computed_date: str
    commodity: str
    country: str
    forecast_type: str
    horizon: str                  # Time horizon being evaluated
    n_forecasts: int              # Number of forecast-actual pairs

    # Core accuracy metrics
    mae: float                    # Mean Absolute Error
    mape: float                   # Mean Absolute Percentage Error
    rmse: float                   # Root Mean Squared Error
    mpe: float                    # Mean Percentage Error (measures bias)

    # Directional accuracy
    directional_accuracy: float   # % of correct direction predictions

    # Bias indicators
    mean_error: float             # Average error (positive = over-forecast)
    median_error: float           # Median error

    # Additional stats
    min_error: float
    max_error: float
    std_error: float              # Standard deviation of errors

    # Theil's U statistic (1.0 = same as naive, <1.0 = better than naive)
    theil_u: float = None

    # Time period covered
    period_start: str = None
    period_end: str = None


class ForecastTracker:
    """Main class for forecast tracking and accuracy measurement."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    def init_database(self):
        """Initialize forecast tracking tables."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.executescript("""
            -- Forecasts table: stores all predictions with vintage tracking
            CREATE TABLE IF NOT EXISTS forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                forecast_id TEXT UNIQUE NOT NULL,
                forecast_date DATE NOT NULL,      -- When forecast was made (vintage)
                target_date DATE NOT NULL,        -- What's being forecasted
                commodity TEXT NOT NULL,
                country TEXT NOT NULL,
                forecast_type TEXT NOT NULL,      -- price, production, etc.
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                confidence_low REAL,
                confidence_high REAL,
                marketing_year TEXT,
                notes TEXT,
                source TEXT DEFAULT 'RLC',
                analyst TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                -- Index for fast lookups
                UNIQUE(forecast_date, target_date, commodity, country, forecast_type, source)
            );

            CREATE INDEX IF NOT EXISTS idx_forecasts_commodity
                ON forecasts(commodity);
            CREATE INDEX IF NOT EXISTS idx_forecasts_target
                ON forecasts(target_date);
            CREATE INDEX IF NOT EXISTS idx_forecasts_vintage
                ON forecasts(forecast_date);
            CREATE INDEX IF NOT EXISTS idx_forecasts_type
                ON forecasts(forecast_type);

            -- Actuals table: stores reported values with revision tracking
            CREATE TABLE IF NOT EXISTS actuals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actual_id TEXT UNIQUE NOT NULL,
                report_date DATE NOT NULL,        -- When the actual was reported
                target_date DATE NOT NULL,        -- What period this represents
                commodity TEXT NOT NULL,
                country TEXT NOT NULL,
                value_type TEXT NOT NULL,         -- Same categories as forecast_type
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                marketing_year TEXT,
                source TEXT,                      -- USDA, CONAB, etc.
                revision_number INTEGER DEFAULT 0,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                -- Allow multiple revisions
                UNIQUE(target_date, commodity, country, value_type, source, revision_number)
            );

            CREATE INDEX IF NOT EXISTS idx_actuals_commodity
                ON actuals(commodity);
            CREATE INDEX IF NOT EXISTS idx_actuals_target
                ON actuals(target_date);
            CREATE INDEX IF NOT EXISTS idx_actuals_type
                ON actuals(value_type);

            -- Accuracy metrics table: stores computed accuracy scores
            CREATE TABLE IF NOT EXISTS accuracy_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_id TEXT UNIQUE NOT NULL,
                computed_date DATE NOT NULL,
                commodity TEXT NOT NULL,
                country TEXT,
                forecast_type TEXT NOT NULL,
                horizon TEXT,                     -- Forecast horizon evaluated
                n_forecasts INTEGER NOT NULL,

                -- Core metrics
                mae REAL,
                mape REAL,
                rmse REAL,
                mpe REAL,                         -- Bias measure

                -- Directional
                directional_accuracy REAL,

                -- Bias
                mean_error REAL,
                median_error REAL,

                -- Range
                min_error REAL,
                max_error REAL,
                std_error REAL,

                -- Advanced
                theil_u REAL,

                -- Period
                period_start DATE,
                period_end DATE,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_metrics_commodity
                ON accuracy_metrics(commodity);
            CREATE INDEX IF NOT EXISTS idx_metrics_type
                ON accuracy_metrics(forecast_type);

            -- Forecast-Actual pairs: links forecasts to their corresponding actuals
            CREATE TABLE IF NOT EXISTS forecast_actual_pairs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                forecast_id TEXT NOT NULL,
                actual_id TEXT NOT NULL,
                error REAL,                       -- actual - forecast
                percentage_error REAL,            -- (actual - forecast) / actual * 100
                absolute_error REAL,
                absolute_percentage_error REAL,
                direction_correct INTEGER,        -- 1 if direction prediction correct, 0 otherwise
                days_ahead INTEGER,               -- How many days ahead was the forecast
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                UNIQUE(forecast_id, actual_id),
                FOREIGN KEY (forecast_id) REFERENCES forecasts(forecast_id),
                FOREIGN KEY (actual_id) REFERENCES actuals(actual_id)
            );

            -- Feedback log: tracks methodology adjustments based on accuracy analysis
            CREATE TABLE IF NOT EXISTS forecast_feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feedback_date DATE NOT NULL,
                commodity TEXT,
                country TEXT,
                forecast_type TEXT,
                issue_identified TEXT,            -- What pattern was found
                root_cause TEXT,                  -- Hypothesized cause
                adjustment_made TEXT,             -- What was changed
                expected_improvement TEXT,        -- What improvement is expected
                analyst TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        conn.commit()
        conn.close()
        print(f"Forecast tracking tables initialized in {self.db_path}")

    def generate_forecast_id(self, forecast: Forecast) -> str:
        """Generate a unique forecast ID."""
        components = [
            forecast.forecast_date,
            forecast.target_date,
            forecast.commodity,
            forecast.country,
            forecast.forecast_type
        ]
        return "_".join(str(c).replace("/", "-").replace(" ", "_") for c in components)

    def generate_actual_id(self, actual: Actual) -> str:
        """Generate a unique actual ID."""
        components = [
            actual.report_date,
            actual.target_date,
            actual.commodity,
            actual.country,
            actual.value_type,
            actual.source or "unknown",
            actual.revision_number
        ]
        return "_".join(str(c).replace("/", "-").replace(" ", "_") for c in components)

    def record_forecast(self, forecast: Forecast) -> str:
        """Record a new forecast."""
        if not forecast.forecast_id:
            forecast.forecast_id = self.generate_forecast_id(forecast)

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO forecasts
            (forecast_id, forecast_date, target_date, commodity, country,
             forecast_type, value, unit, confidence_low, confidence_high,
             marketing_year, notes, source, analyst)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            forecast.forecast_id,
            forecast.forecast_date,
            forecast.target_date,
            forecast.commodity,
            forecast.country,
            forecast.forecast_type,
            forecast.value,
            forecast.unit,
            forecast.confidence_low,
            forecast.confidence_high,
            forecast.marketing_year,
            forecast.notes,
            forecast.source,
            forecast.analyst
        ))

        conn.commit()
        conn.close()

        print(f"Recorded forecast: {forecast.forecast_id}")
        return forecast.forecast_id

    def record_actual(self, actual: Actual) -> str:
        """Record an actual value."""
        if not actual.actual_id:
            actual.actual_id = self.generate_actual_id(actual)

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO actuals
            (actual_id, report_date, target_date, commodity, country,
             value_type, value, unit, marketing_year, source, revision_number, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            actual.actual_id,
            actual.report_date,
            actual.target_date,
            actual.commodity,
            actual.country,
            actual.value_type,
            actual.value,
            actual.unit,
            actual.marketing_year,
            actual.source,
            actual.revision_number,
            actual.notes
        ))

        conn.commit()
        conn.close()

        print(f"Recorded actual: {actual.actual_id}")
        return actual.actual_id

    def match_forecasts_to_actuals(self):
        """Match forecasts to their corresponding actuals and compute errors."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        # Find matching pairs based on target_date, commodity, country, type
        cursor.execute("""
            SELECT
                f.forecast_id,
                f.forecast_date,
                f.target_date,
                f.commodity,
                f.country,
                f.forecast_type,
                f.value as forecast_value,
                a.actual_id,
                a.report_date,
                a.value as actual_value
            FROM forecasts f
            JOIN actuals a ON
                f.target_date = a.target_date AND
                f.commodity = a.commodity AND
                f.country = a.country AND
                f.forecast_type = a.value_type
            WHERE NOT EXISTS (
                SELECT 1 FROM forecast_actual_pairs p
                WHERE p.forecast_id = f.forecast_id AND p.actual_id = a.actual_id
            )
        """)

        pairs = cursor.fetchall()

        for pair in pairs:
            (forecast_id, forecast_date, target_date, commodity, country,
             forecast_type, forecast_value, actual_id, report_date, actual_value) = pair

            # Compute errors
            error = actual_value - forecast_value
            abs_error = abs(error)

            if actual_value != 0:
                pct_error = (error / actual_value) * 100
                abs_pct_error = abs(pct_error)
            else:
                pct_error = None
                abs_pct_error = None

            # Compute days ahead
            try:
                forecast_dt = datetime.strptime(forecast_date, "%Y-%m-%d")
                target_dt = datetime.strptime(target_date, "%Y-%m-%d")
                days_ahead = (target_dt - forecast_dt).days
            except:
                days_ahead = None

            # Direction correctness (requires previous actual)
            cursor.execute("""
                SELECT value FROM actuals
                WHERE commodity = ? AND country = ? AND value_type = ?
                    AND target_date < ?
                ORDER BY target_date DESC LIMIT 1
            """, (commodity, country, forecast_type, target_date))

            prev_actual = cursor.fetchone()
            direction_correct = None
            if prev_actual:
                prev_value = prev_actual[0]
                actual_direction = 1 if actual_value > prev_value else (-1 if actual_value < prev_value else 0)
                forecast_direction = 1 if forecast_value > prev_value else (-1 if forecast_value < prev_value else 0)
                direction_correct = 1 if actual_direction == forecast_direction else 0

            # Insert pair
            cursor.execute("""
                INSERT OR REPLACE INTO forecast_actual_pairs
                (forecast_id, actual_id, error, percentage_error, absolute_error,
                 absolute_percentage_error, direction_correct, days_ahead)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                forecast_id, actual_id, error, pct_error, abs_error,
                abs_pct_error, direction_correct, days_ahead
            ))

        conn.commit()
        conn.close()

        print(f"Matched {len(pairs)} new forecast-actual pairs")
        return len(pairs)

    def compute_accuracy_metrics(self, commodity: str = None, country: str = None,
                                  forecast_type: str = None,
                                  period_start: str = None, period_end: str = None) -> AccuracyMetrics:
        """Compute accuracy metrics for a set of forecasts."""
        conn = sqlite3.connect(str(self.db_path))

        # Build query
        query = """
            SELECT
                p.error,
                p.percentage_error,
                p.absolute_error,
                p.absolute_percentage_error,
                p.direction_correct,
                p.days_ahead,
                f.value as forecast_value,
                a.value as actual_value,
                f.commodity,
                f.country,
                f.forecast_type
            FROM forecast_actual_pairs p
            JOIN forecasts f ON p.forecast_id = f.forecast_id
            JOIN actuals a ON p.actual_id = a.actual_id
            WHERE 1=1
        """
        params = []

        if commodity:
            query += " AND f.commodity = ?"
            params.append(commodity)
        if country:
            query += " AND f.country = ?"
            params.append(country)
        if forecast_type:
            query += " AND f.forecast_type = ?"
            params.append(forecast_type)
        if period_start:
            query += " AND f.target_date >= ?"
            params.append(period_start)
        if period_end:
            query += " AND f.target_date <= ?"
            params.append(period_end)

        df = pd.read_sql(query, conn, params=params)
        conn.close()

        if len(df) == 0:
            print("No forecast-actual pairs found matching criteria")
            return None

        # Compute metrics
        n = len(df)
        errors = df['error'].dropna()
        pct_errors = df['percentage_error'].dropna()
        abs_errors = df['absolute_error'].dropna()
        abs_pct_errors = df['absolute_percentage_error'].dropna()

        # Core metrics
        mae = abs_errors.mean() if len(abs_errors) > 0 else None
        mape = abs_pct_errors.mean() if len(abs_pct_errors) > 0 else None
        rmse = math.sqrt((errors ** 2).mean()) if len(errors) > 0 else None
        mpe = pct_errors.mean() if len(pct_errors) > 0 else None  # Bias measure

        # Directional accuracy
        direction_vals = df['direction_correct'].dropna()
        directional_accuracy = direction_vals.mean() * 100 if len(direction_vals) > 0 else None

        # Bias indicators
        mean_error = errors.mean() if len(errors) > 0 else None
        median_error = errors.median() if len(errors) > 0 else None

        # Range
        min_error = errors.min() if len(errors) > 0 else None
        max_error = errors.max() if len(errors) > 0 else None
        std_error = errors.std() if len(errors) > 0 else None

        # Theil's U statistic (compare to naive forecast)
        # Naive forecast: use previous actual as forecast
        if len(df) > 1:
            forecast_sq_errors = (df['actual_value'] - df['forecast_value']) ** 2
            # Naive: assume no change
            naive_sq_errors = df['actual_value'].diff() ** 2

            if naive_sq_errors.sum() > 0:
                theil_u = math.sqrt(forecast_sq_errors.mean() / naive_sq_errors.mean())
            else:
                theil_u = None
        else:
            theil_u = None

        metric_id = f"metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        metrics = AccuracyMetrics(
            metric_id=metric_id,
            computed_date=datetime.now().strftime("%Y-%m-%d"),
            commodity=commodity or "all",
            country=country or "all",
            forecast_type=forecast_type or "all",
            horizon="all",
            n_forecasts=n,
            mae=mae,
            mape=mape,
            rmse=rmse,
            mpe=mpe,
            directional_accuracy=directional_accuracy,
            mean_error=mean_error,
            median_error=median_error,
            min_error=min_error,
            max_error=max_error,
            std_error=std_error,
            theil_u=theil_u,
            period_start=period_start,
            period_end=period_end
        )

        # Save to database
        self._save_metrics(metrics)

        return metrics

    def _save_metrics(self, metrics: AccuracyMetrics):
        """Save accuracy metrics to database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO accuracy_metrics
            (metric_id, computed_date, commodity, country, forecast_type, horizon,
             n_forecasts, mae, mape, rmse, mpe, directional_accuracy,
             mean_error, median_error, min_error, max_error, std_error, theil_u,
             period_start, period_end)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            metrics.metric_id, metrics.computed_date, metrics.commodity,
            metrics.country, metrics.forecast_type, metrics.horizon,
            metrics.n_forecasts, metrics.mae, metrics.mape, metrics.rmse,
            metrics.mpe, metrics.directional_accuracy, metrics.mean_error,
            metrics.median_error, metrics.min_error, metrics.max_error,
            metrics.std_error, metrics.theil_u, metrics.period_start, metrics.period_end
        ))

        conn.commit()
        conn.close()

    def analyze_bias(self, commodity: str = None) -> pd.DataFrame:
        """Analyze systematic bias in forecasts."""
        conn = sqlite3.connect(str(self.db_path))

        query = """
            SELECT
                f.commodity,
                f.country,
                f.forecast_type,
                COUNT(*) as n_forecasts,
                AVG(p.error) as mean_error,
                AVG(p.percentage_error) as mean_pct_error,
                -- Bias direction
                CASE
                    WHEN AVG(p.percentage_error) > 2 THEN 'Over-forecasting'
                    WHEN AVG(p.percentage_error) < -2 THEN 'Under-forecasting'
                    ELSE 'Relatively unbiased'
                END as bias_direction,
                -- Consistency of bias
                STDEV(p.percentage_error) as pct_error_std,
                MIN(p.percentage_error) as min_pct_error,
                MAX(p.percentage_error) as max_pct_error
            FROM forecast_actual_pairs p
            JOIN forecasts f ON p.forecast_id = f.forecast_id
            WHERE p.percentage_error IS NOT NULL
        """

        if commodity:
            query += f" AND f.commodity = '{commodity}'"

        query += """
            GROUP BY f.commodity, f.country, f.forecast_type
            HAVING COUNT(*) >= 3
            ORDER BY ABS(AVG(p.percentage_error)) DESC
        """

        df = pd.read_sql(query, conn)
        conn.close()

        return df

    def generate_accuracy_report(self, output_format: str = "markdown") -> str:
        """Generate a comprehensive accuracy report."""
        conn = sqlite3.connect(str(self.db_path))

        # Get overall stats
        overall = pd.read_sql("""
            SELECT
                COUNT(*) as total_forecasts,
                AVG(absolute_percentage_error) as overall_mape,
                AVG(CASE WHEN direction_correct = 1 THEN 100 ELSE 0 END) as directional_accuracy
            FROM forecast_actual_pairs
            WHERE absolute_percentage_error IS NOT NULL
        """, conn)

        # Get by commodity
        by_commodity = pd.read_sql("""
            SELECT
                f.commodity,
                COUNT(*) as n_forecasts,
                AVG(p.absolute_percentage_error) as mape,
                AVG(CASE WHEN p.direction_correct = 1 THEN 100 ELSE 0 END) as directional_accuracy,
                AVG(p.percentage_error) as bias
            FROM forecast_actual_pairs p
            JOIN forecasts f ON p.forecast_id = f.forecast_id
            GROUP BY f.commodity
            ORDER BY COUNT(*) DESC
        """, conn)

        # Get by forecast type
        by_type = pd.read_sql("""
            SELECT
                f.forecast_type,
                COUNT(*) as n_forecasts,
                AVG(p.absolute_percentage_error) as mape,
                AVG(CASE WHEN p.direction_correct = 1 THEN 100 ELSE 0 END) as directional_accuracy
            FROM forecast_actual_pairs p
            JOIN forecasts f ON p.forecast_id = f.forecast_id
            GROUP BY f.forecast_type
            ORDER BY COUNT(*) DESC
        """, conn)

        # Get recent performance trend
        trend = pd.read_sql("""
            SELECT
                strftime('%Y-%m', f.target_date) as month,
                COUNT(*) as n_forecasts,
                AVG(p.absolute_percentage_error) as mape
            FROM forecast_actual_pairs p
            JOIN forecasts f ON p.forecast_id = f.forecast_id
            WHERE f.target_date >= date('now', '-12 months')
            GROUP BY strftime('%Y-%m', f.target_date)
            ORDER BY month
        """, conn)

        conn.close()

        # Generate report
        if output_format == "markdown":
            report = self._format_markdown_report(overall, by_commodity, by_type, trend)
        else:
            report = json.dumps({
                "overall": overall.to_dict(orient='records'),
                "by_commodity": by_commodity.to_dict(orient='records'),
                "by_type": by_type.to_dict(orient='records'),
                "trend": trend.to_dict(orient='records')
            }, indent=2)

        # Save report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = REPORTS_DIR / f"accuracy_report_{timestamp}.{'md' if output_format == 'markdown' else 'json'}"
        report_file.write_text(report)

        print(f"Report saved to: {report_file}")
        return report

    def _format_markdown_report(self, overall, by_commodity, by_type, trend) -> str:
        """Format accuracy report as markdown."""
        report = f"""# RLC Forecast Accuracy Report
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Executive Summary

| Metric | Value |
|--------|-------|
| Total Forecasts Evaluated | {int(overall['total_forecasts'].iloc[0]) if len(overall) > 0 else 0:,} |
| Overall MAPE | {overall['overall_mape'].iloc[0]:.2f}% |
| Directional Accuracy | {overall['directional_accuracy'].iloc[0]:.1f}% |

## Performance by Commodity

| Commodity | N Forecasts | MAPE | Directional Accuracy | Bias |
|-----------|-------------|------|---------------------|------|
"""
        for _, row in by_commodity.iterrows():
            bias = "Over" if row['bias'] > 0 else "Under" if row['bias'] < 0 else "Neutral"
            report += f"| {row['commodity']} | {int(row['n_forecasts'])} | {row['mape']:.2f}% | {row['directional_accuracy']:.1f}% | {bias} ({row['bias']:.1f}%) |\n"

        report += """
## Performance by Forecast Type

| Type | N Forecasts | MAPE | Directional Accuracy |
|------|-------------|------|---------------------|
"""
        for _, row in by_type.iterrows():
            report += f"| {row['forecast_type']} | {int(row['n_forecasts'])} | {row['mape']:.2f}% | {row['directional_accuracy']:.1f}% |\n"

        report += """
## Performance Trend (Last 12 Months)

| Month | Forecasts | MAPE |
|-------|-----------|------|
"""
        for _, row in trend.iterrows():
            report += f"| {row['month']} | {int(row['n_forecasts'])} | {row['mape']:.2f}% |\n"

        report += """
---

## Methodology

### Metrics Explained

- **MAPE (Mean Absolute Percentage Error)**: Average of absolute percentage errors. Lower is better.
- **Directional Accuracy**: Percentage of forecasts that correctly predicted the direction of change.
- **Bias**: Positive = over-forecasting, Negative = under-forecasting.

### Interpretation Guide

| MAPE | Interpretation |
|------|----------------|
| < 5% | Excellent accuracy |
| 5-10% | Good accuracy |
| 10-20% | Reasonable accuracy |
| 20-30% | Fair accuracy |
| > 30% | Poor accuracy |

| Directional Accuracy | Interpretation |
|---------------------|----------------|
| > 70% | Strong predictive power |
| 60-70% | Good predictive power |
| 50-60% | Marginal predictive power |
| < 50% | No better than random |
"""
        return report

    def export_for_powerbi(self) -> Path:
        """Export data for PowerBI visualization."""
        conn = sqlite3.connect(str(self.db_path))

        # Detailed forecast-actual pairs
        df_pairs = pd.read_sql("""
            SELECT
                f.forecast_id,
                f.forecast_date,
                f.target_date,
                f.commodity,
                f.country,
                f.forecast_type,
                f.value as forecast_value,
                f.unit,
                f.marketing_year,
                a.value as actual_value,
                a.source as actual_source,
                p.error,
                p.percentage_error,
                p.absolute_percentage_error,
                p.direction_correct,
                p.days_ahead
            FROM forecast_actual_pairs p
            JOIN forecasts f ON p.forecast_id = f.forecast_id
            JOIN actuals a ON p.actual_id = a.actual_id
        """, conn)

        # Accuracy summary by month
        df_monthly = pd.read_sql("""
            SELECT
                strftime('%Y-%m', f.target_date) as target_month,
                f.commodity,
                f.forecast_type,
                COUNT(*) as n_forecasts,
                AVG(p.absolute_percentage_error) as mape,
                AVG(p.percentage_error) as mpe,
                AVG(CASE WHEN p.direction_correct = 1 THEN 100 ELSE 0 END) as directional_accuracy
            FROM forecast_actual_pairs p
            JOIN forecasts f ON p.forecast_id = f.forecast_id
            GROUP BY target_month, f.commodity, f.forecast_type
        """, conn)

        conn.close()

        # Export to Excel
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = REPORTS_DIR / f"forecast_accuracy_powerbi_{timestamp}.xlsx"

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df_pairs.to_excel(writer, sheet_name='Forecast_Actual_Pairs', index=False)
            df_monthly.to_excel(writer, sheet_name='Monthly_Summary', index=False)

        print(f"Exported to: {output_file}")
        return output_file

    def record_feedback(self, commodity: str, forecast_type: str,
                        issue: str, root_cause: str, adjustment: str,
                        expected_improvement: str, analyst: str = None):
        """Record a methodology adjustment based on accuracy analysis."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO forecast_feedback
            (feedback_date, commodity, forecast_type, issue_identified,
             root_cause, adjustment_made, expected_improvement, analyst)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().strftime("%Y-%m-%d"),
            commodity, forecast_type, issue, root_cause, adjustment,
            expected_improvement, analyst
        ))

        conn.commit()
        conn.close()

        print(f"Recorded feedback for {commodity} {forecast_type}")

    def get_improvement_suggestions(self) -> List[Dict]:
        """Analyze patterns and suggest improvements."""
        bias_df = self.analyze_bias()

        suggestions = []

        for _, row in bias_df.iterrows():
            if row['bias_direction'] != 'Relatively unbiased':
                suggestion = {
                    "commodity": row['commodity'],
                    "country": row['country'],
                    "forecast_type": row['forecast_type'],
                    "issue": row['bias_direction'],
                    "mean_bias": f"{row['mean_pct_error']:.2f}%",
                    "suggestion": self._generate_suggestion(row)
                }
                suggestions.append(suggestion)

        return suggestions

    def _generate_suggestion(self, row) -> str:
        """Generate a suggestion based on bias pattern."""
        bias = row['mean_pct_error']
        forecast_type = row['forecast_type']

        if bias > 5:  # Over-forecasting
            if forecast_type == 'production':
                return "Consider applying a yield discount factor or reviewing planted area assumptions"
            elif forecast_type == 'price':
                return "Review demand assumptions; may be overestimating demand strength"
            elif forecast_type == 'exports':
                return "Consider logistics constraints and competitor supply more carefully"
            else:
                return f"Apply a {abs(bias):.1f}% downward adjustment to initial estimates"
        elif bias < -5:  # Under-forecasting
            if forecast_type == 'production':
                return "May be too conservative on yields; review technology/weather assumptions"
            elif forecast_type == 'price':
                return "Review supply assumptions; may be overestimating supply availability"
            elif forecast_type == 'exports':
                return "May be underestimating demand; review importing country needs"
            else:
                return f"Apply a {abs(bias):.1f}% upward adjustment to initial estimates"
        else:
            return "No significant bias detected"


def interactive_record_forecast(tracker: ForecastTracker):
    """Interactive mode to record a forecast."""
    print("\n" + "="*60)
    print("  RECORD NEW FORECAST")
    print("="*60 + "\n")

    commodity = input("Commodity (e.g., soybeans, corn): ").strip().lower()
    country = input("Country (e.g., US, Brazil, World): ").strip()

    print("\nForecast types: price, production, ending_stocks, exports, crush, yield, area")
    forecast_type = input("Forecast type: ").strip().lower()

    value = float(input("Forecast value: "))
    unit = input("Unit (e.g., USD/bushel, MMT, 1000 ha): ").strip()

    target_date = input("Target date (YYYY-MM-DD): ").strip()
    marketing_year = input("Marketing year (e.g., 2023/24, or leave blank): ").strip() or None
    notes = input("Notes (optional): ").strip() or None

    forecast = Forecast(
        forecast_id=None,
        forecast_date=datetime.now().strftime("%Y-%m-%d"),
        target_date=target_date,
        commodity=commodity,
        country=country,
        forecast_type=forecast_type,
        value=value,
        unit=unit,
        marketing_year=marketing_year,
        notes=notes
    )

    tracker.record_forecast(forecast)


def interactive_record_actual(tracker: ForecastTracker):
    """Interactive mode to record an actual."""
    print("\n" + "="*60)
    print("  RECORD ACTUAL VALUE")
    print("="*60 + "\n")

    commodity = input("Commodity: ").strip().lower()
    country = input("Country: ").strip()

    print("\nValue types: price, production, ending_stocks, exports, crush, yield, area")
    value_type = input("Value type: ").strip().lower()

    value = float(input("Actual value: "))
    unit = input("Unit: ").strip()

    target_date = input("Target date (YYYY-MM-DD): ").strip()
    source = input("Source (e.g., USDA, CONAB): ").strip()
    marketing_year = input("Marketing year (optional): ").strip() or None

    actual = Actual(
        actual_id=None,
        report_date=datetime.now().strftime("%Y-%m-%d"),
        target_date=target_date,
        commodity=commodity,
        country=country,
        value_type=value_type,
        value=value,
        unit=unit,
        marketing_year=marketing_year,
        source=source
    )

    tracker.record_actual(actual)


def main():
    parser = argparse.ArgumentParser(description='RLC Forecast Tracking System')
    parser.add_argument('--init', action='store_true', help='Initialize database tables')
    parser.add_argument('--record-forecast', action='store_true', help='Record a new forecast')
    parser.add_argument('--record-actual', action='store_true', help='Record an actual value')
    parser.add_argument('--match', action='store_true', help='Match forecasts to actuals')
    parser.add_argument('--compute-accuracy', action='store_true', help='Compute accuracy metrics')
    parser.add_argument('--report', action='store_true', help='Generate accuracy report')
    parser.add_argument('--analyze-bias', action='store_true', help='Analyze forecast bias')
    parser.add_argument('--suggestions', action='store_true', help='Get improvement suggestions')
    parser.add_argument('--export-powerbi', action='store_true', help='Export for PowerBI')
    parser.add_argument('--commodity', type=str, help='Filter by commodity')
    parser.add_argument('--country', type=str, help='Filter by country')

    args = parser.parse_args()

    tracker = ForecastTracker()

    if args.init:
        tracker.init_database()
        return

    if args.record_forecast:
        interactive_record_forecast(tracker)
        return

    if args.record_actual:
        interactive_record_actual(tracker)
        return

    if args.match:
        tracker.match_forecasts_to_actuals()
        return

    if args.compute_accuracy:
        metrics = tracker.compute_accuracy_metrics(
            commodity=args.commodity,
            country=args.country
        )
        if metrics:
            print(f"\n  ACCURACY METRICS")
            print(f"  {'='*40}")
            print(f"  Forecasts analyzed: {metrics.n_forecasts}")
            print(f"  MAPE: {metrics.mape:.2f}%")
            print(f"  RMSE: {metrics.rmse:.4f}")
            print(f"  Directional Accuracy: {metrics.directional_accuracy:.1f}%")
            print(f"  Mean Bias: {metrics.mpe:.2f}%")
            if metrics.theil_u:
                print(f"  Theil's U: {metrics.theil_u:.3f} ({'better' if metrics.theil_u < 1 else 'worse'} than naive)")
        return

    if args.report:
        report = tracker.generate_accuracy_report()
        print("\n" + report)
        return

    if args.analyze_bias:
        bias_df = tracker.analyze_bias(commodity=args.commodity)
        print("\n  BIAS ANALYSIS")
        print("  " + "="*60)
        print(bias_df.to_string(index=False))
        return

    if args.suggestions:
        suggestions = tracker.get_improvement_suggestions()
        print("\n  IMPROVEMENT SUGGESTIONS")
        print("  " + "="*60)
        for s in suggestions:
            print(f"\n  {s['commodity']} - {s['country']} - {s['forecast_type']}")
            print(f"    Issue: {s['issue']} ({s['mean_bias']})")
            print(f"    Suggestion: {s['suggestion']}")
        return

    if args.export_powerbi:
        tracker.export_for_powerbi()
        return

    parser.print_help()


if __name__ == '__main__':
    main()
