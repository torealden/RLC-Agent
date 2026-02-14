"""
Multi-Model Yield Prediction Engine

Three sub-models combined in a growth-stage-weighted ensemble:
  A. Trend-Adjusted Regression — baseline, interpretable (4–6 features)
  B. Gradient Boosting — nonlinear interactions, all features
  C. Analog Year — "this year looks like…" narrative

Usage:
    python -m src.models.yield_prediction_model train --crop corn
    python -m src.models.yield_prediction_model predict --crop corn --year 2026 --week 26
"""

import argparse
import json
import logging
import os
import pickle
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

MODEL_DIR = PROJECT_ROOT / "models" / "yield"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

# Features used by Model A (interpretable subset)
MODEL_A_FEATURES = [
    'gdd_vs_normal_pct', 'precip_vs_normal_pct', 'good_excellent_pct',
    'stress_days_heat', 'condition_index', 'stress_days_drought',
]

# Features used for analog distance
ANALOG_FEATURES = [
    'gdd_vs_normal_pct', 'precip_vs_normal_pct', 'stress_days_heat',
    'stress_days_drought', 'condition_index', 'good_excellent_pct',
]

# All numeric features from yield_features table
ALL_NUMERIC_FEATURES = [
    'gdd_cum', 'gdd_vs_normal_pct', 'precip_cum_mm', 'precip_vs_normal_pct',
    'stress_days_heat', 'stress_days_drought', 'excess_moisture_days', 'frost_events',
    'tmax_weekly_avg', 'tmin_weekly_avg', 'tavg_weekly',
    'ndvi_mean', 'ndvi_anomaly', 'ndvi_trend_4wk',
    'condition_index', 'condition_vs_5yr', 'progress_index', 'progress_vs_normal',
    'pct_planted', 'pct_emerged', 'pct_silking', 'pct_dough',
    'pct_mature', 'pct_harvested', 'good_excellent_pct',
    'ww_risk_score', 'ww_outlook_sentiment',
]

# Ensemble weight schedule by growth phase
ENSEMBLE_WEIGHTS = {
    'corn': {
        'pre_planting': {'model_a': 0.6, 'model_b': 0.2, 'model_c': 0.2},
        'planting':     {'model_a': 0.5, 'model_b': 0.25, 'model_c': 0.25},
        'vegetative':   {'model_a': 0.4, 'model_b': 0.35, 'model_c': 0.25},
        'reproductive': {'model_a': 0.2, 'model_b': 0.5, 'model_c': 0.3},
        'maturity':     {'model_a': 0.3, 'model_b': 0.4, 'model_c': 0.3},
    },
    'soybeans': {
        'pre_planting': {'model_a': 0.6, 'model_b': 0.2, 'model_c': 0.2},
        'planting':     {'model_a': 0.5, 'model_b': 0.25, 'model_c': 0.25},
        'vegetative':   {'model_a': 0.4, 'model_b': 0.35, 'model_c': 0.25},
        'reproductive': {'model_a': 0.2, 'model_b': 0.5, 'model_c': 0.3},
        'maturity':     {'model_a': 0.3, 'model_b': 0.4, 'model_c': 0.3},
    },
    'winter_wheat': {
        'pre_planting': {'model_a': 0.7, 'model_b': 0.15, 'model_c': 0.15},
        'planting':     {'model_a': 0.6, 'model_b': 0.2, 'model_c': 0.2},
        'vegetative':   {'model_a': 0.4, 'model_b': 0.35, 'model_c': 0.25},
        'reproductive': {'model_a': 0.2, 'model_b': 0.5, 'model_c': 0.3},
        'maturity':     {'model_a': 0.3, 'model_b': 0.4, 'model_c': 0.3},
    },
    'cotton': {
        'pre_planting': {'model_a': 0.6, 'model_b': 0.2, 'model_c': 0.2},
        'planting':     {'model_a': 0.5, 'model_b': 0.25, 'model_c': 0.25},
        'vegetative':   {'model_a': 0.35, 'model_b': 0.4, 'model_c': 0.25},
        'reproductive': {'model_a': 0.2, 'model_b': 0.5, 'model_c': 0.3},
        'maturity':     {'model_a': 0.3, 'model_b': 0.4, 'model_c': 0.3},
    },
}

# Confidence multiplier by week (earlier → less confident)
CONFIDENCE_BY_WEEK = {
    10: 0.30, 15: 0.40, 18: 0.45, 20: 0.50, 22: 0.55, 24: 0.60,
    26: 0.70, 28: 0.75, 30: 0.80, 32: 0.85, 34: 0.88, 36: 0.90,
    38: 0.93, 40: 0.95,
}


@dataclass
class YieldPrediction:
    """Single yield prediction for a state/crop/week."""
    commodity: str
    state: str
    year: int
    forecast_week: int
    forecast_date: date
    yield_forecast: float
    yield_low: float
    yield_high: float
    trend_yield: float
    vs_trend_pct: float
    last_year_yield: Optional[float] = None
    vs_last_year_pct: Optional[float] = None
    model_type: str = 'ensemble'
    confidence: float = 0.5
    primary_driver: str = ''
    analog_years: str = ''
    feature_importance: dict = field(default_factory=dict)


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


class YieldPredictionModel:
    """Multi-model ensemble for crop yield prediction."""

    def __init__(self, model_dir: Path = None):
        self.model_dir = model_dir or MODEL_DIR

    # ------------------------------------------------------------------
    # TRAINING
    # ------------------------------------------------------------------

    def train(self, crop: str, train_years: range = range(2005, 2024),
              target_week: int = 30) -> dict:
        """
        Train all three sub-models for a given crop at a target week.

        Returns training metrics dict.
        """
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.linear_model import LinearRegression
        from sklearn.preprocessing import StandardScaler

        conn = get_db_connection()
        cur = conn.cursor()

        try:
            # Build training data: features at target_week + actual yield
            crop_db = {'corn': 'CORN', 'soybeans': 'SOYBEANS',
                       'winter_wheat': 'WHEAT_ALL', 'cotton': 'COTTON'}.get(crop, crop.upper())

            feature_cols = ", ".join(ALL_NUMERIC_FEATURES)
            cur.execute(f"""
                SELECT f.state, f.year, {feature_cols}, y.yield_per_acre
                FROM silver.yield_features f
                JOIN bronze.nass_state_yields y
                    ON f.state = y.state_abbrev
                    AND f.year = y.year
                    AND y.commodity = %s
                WHERE f.crop = %s
                  AND f.week = %s
                  AND f.year >= %s AND f.year <= %s
                  AND y.yield_per_acre IS NOT NULL
                ORDER BY f.year, f.state
            """, (crop_db, crop, target_week, min(train_years), max(train_years)))
            rows = cur.fetchall()

            if len(rows) < 20:
                logger.warning(f"Only {len(rows)} training samples for {crop} week {target_week}")
                if len(rows) < 5:
                    logger.error("Not enough training data")
                    return {'error': 'insufficient_data', 'n_samples': len(rows)}

            states = [r[0] for r in rows]
            years = np.array([r[1] for r in rows])
            n_features = len(ALL_NUMERIC_FEATURES)
            X_raw = np.array([[r[i + 2] for i in range(n_features)] for r in rows], dtype=float)
            y = np.array([r[n_features + 2] for r in rows], dtype=float)

            # Replace NaN with 0
            X_raw = np.nan_to_num(X_raw, nan=0.0)

            # Get trend yields for each sample
            # Note: yield_trend stores full state names, features use abbreviations
            from src.models.yield_feature_engine import US_STATES
            trend_yields = np.zeros(len(rows))
            for i, (state, year) in enumerate(zip(states, years)):
                state_full = US_STATES.get(state, state)
                cur.execute("""
                    SELECT trend_yield_current, slope, intercept
                    FROM silver.yield_trend
                    WHERE commodity = %s AND state = %s AND trend_type = 'linear'
                """, (crop_db, state_full))
                trend_row = cur.fetchone()
                if trend_row and trend_row[2] is not None:
                    trend_yields[i] = float(trend_row[2]) + float(trend_row[1]) * year
                else:
                    trend_yields[i] = np.mean(y)

            # Yield deviations from trend
            y_deviation = y - trend_yields

            # Scale features
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X_raw)

            # --- Model A: Trend-Adjusted Regression ---
            a_idx = [ALL_NUMERIC_FEATURES.index(f) for f in MODEL_A_FEATURES
                     if f in ALL_NUMERIC_FEATURES]
            X_a = X_raw[:, a_idx]
            X_a = np.nan_to_num(X_a, nan=0.0)
            model_a = LinearRegression()
            model_a.fit(X_a, y_deviation)

            # --- Model B: Gradient Boosting ---
            model_b = GradientBoostingRegressor(
                n_estimators=200, max_depth=4, learning_rate=0.1,
                min_samples_leaf=max(3, len(rows) // 20),
                subsample=0.8, random_state=42,
            )
            model_b.fit(X_scaled, y_deviation)

            # Feature importance
            importances = dict(zip(ALL_NUMERIC_FEATURES, model_b.feature_importances_))
            top_features = sorted(importances.items(), key=lambda x: x[1], reverse=True)[:10]

            # --- Model C: Store feature profiles for analog lookup ---
            analog_data = {
                'states': states,
                'years': years.tolist(),
                'yields': y.tolist(),
                'trend_yields': trend_yields.tolist(),
                'features': X_raw.tolist(),
                'feature_names': ALL_NUMERIC_FEATURES,
            }

            # --- Cross-validation (leave-one-year-out) ---
            unique_years = sorted(set(years))
            cv_errors = []
            for test_year in unique_years:
                train_mask = years != test_year
                test_mask = years == test_year
                if sum(test_mask) == 0 or sum(train_mask) < 10:
                    continue

                # Model B on fold
                gb = GradientBoostingRegressor(
                    n_estimators=200, max_depth=4, learning_rate=0.1,
                    min_samples_leaf=max(3, sum(train_mask) // 20),
                    subsample=0.8, random_state=42,
                )
                s = StandardScaler()
                X_train_s = s.fit_transform(X_raw[train_mask])
                X_test_s = s.transform(X_raw[test_mask])
                gb.fit(X_train_s, y_deviation[train_mask])
                pred_dev = gb.predict(X_test_s)
                pred_yield = trend_yields[test_mask] + pred_dev
                actual_yield = y[test_mask]
                errors = pred_yield - actual_yield
                cv_errors.extend(errors.tolist())

            rmse = np.sqrt(np.mean(np.array(cv_errors) ** 2)) if cv_errors else None
            mae = np.mean(np.abs(cv_errors)) if cv_errors else None
            r2 = 1 - (np.sum(np.array(cv_errors) ** 2) / np.sum((y - np.mean(y)) ** 2)) if cv_errors else None

            # Save models
            with open(self.model_dir / f"{crop}_model_a.pkl", 'wb') as f:
                pickle.dump(model_a, f)
            with open(self.model_dir / f"{crop}_model_b.pkl", 'wb') as f:
                pickle.dump(model_b, f)
            with open(self.model_dir / f"{crop}_scaler.pkl", 'wb') as f:
                pickle.dump(scaler, f)
            with open(self.model_dir / f"{crop}_analog_data.pkl", 'wb') as f:
                pickle.dump(analog_data, f)

            metadata = {
                'crop': crop,
                'target_week': target_week,
                'n_samples': len(rows),
                'train_years': f"{min(train_years)}-{max(train_years)}",
                'feature_names': ALL_NUMERIC_FEATURES,
                'model_a_features': MODEL_A_FEATURES,
                'top_features_b': top_features,
                'rmse_cv': round(rmse, 2) if rmse else None,
                'mae_cv': round(mae, 2) if mae else None,
                'r2_cv': round(r2, 3) if r2 else None,
                'trained_at': datetime.now().isoformat(),
            }
            with open(self.model_dir / f"{crop}_metadata.json", 'w') as f:
                json.dump(metadata, f, indent=2)

            logger.info(f"Training complete for {crop} week {target_week}")
            logger.info(f"  Samples: {len(rows)}, RMSE: {rmse:.2f}, MAE: {mae:.2f}, R²: {r2:.3f}")
            logger.info(f"  Top features: {[f[0] for f in top_features[:5]]}")

            return metadata

        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # PREDICTION
    # ------------------------------------------------------------------

    def predict(self, crop: str, year: int, week: int,
                states: list = None) -> List[YieldPrediction]:
        """Generate yield predictions for current conditions."""
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            # Load trained models
            model_a = self._load_pickle(f"{crop}_model_a.pkl")
            model_b = self._load_pickle(f"{crop}_model_b.pkl")
            scaler = self._load_pickle(f"{crop}_scaler.pkl")
            analog_data = self._load_pickle(f"{crop}_analog_data.pkl")
            metadata = self._load_json(f"{crop}_metadata.json")

            if model_a is None or model_b is None:
                logger.error(f"No trained models found for {crop}")
                return []

            crop_db = {'corn': 'CORN', 'soybeans': 'SOYBEANS',
                       'winter_wheat': 'WHEAT_ALL', 'cotton': 'COTTON'}.get(crop, crop.upper())

            # Get features for current week
            feature_cols = ", ".join(ALL_NUMERIC_FEATURES)
            state_filter = ""
            params = [crop, week, year]
            if states:
                state_filter = "AND f.state = ANY(%s)"
                params.append(states)

            cur.execute(f"""
                SELECT f.state, f.growth_stage, {feature_cols}
                FROM silver.yield_features f
                WHERE f.crop = %s AND f.week = %s AND f.year = %s
                {state_filter}
                ORDER BY f.state
            """, params)
            rows = cur.fetchall()

            if not rows:
                logger.warning(f"No features found for {crop} year={year} week={week}")
                return []

            predictions = []
            from src.models.yield_feature_engine import nass_week_to_date
            forecast_date = nass_week_to_date(year, week)

            for row in rows:
                state = row[0]
                growth_stage = row[1] or 'vegetative'
                features = np.array([row[i + 2] for i in range(len(ALL_NUMERIC_FEATURES))], dtype=float)
                features = np.nan_to_num(features, nan=0.0)

                # Get trend yield (yield_trend stores full state name, features use abbreviation)
                from src.models.yield_feature_engine import US_STATES
                state_full = US_STATES.get(state, state)
                cur.execute("""
                    SELECT slope, intercept, trend_yield_current
                    FROM silver.yield_trend
                    WHERE commodity = %s AND state = %s AND trend_type = 'linear'
                """, (crop_db, state_full))
                trend_row = cur.fetchone()
                if not trend_row:
                    continue
                trend_yield = float(trend_row[0]) * year + float(trend_row[1])

                # Model A prediction
                a_idx = [ALL_NUMERIC_FEATURES.index(f) for f in MODEL_A_FEATURES
                         if f in ALL_NUMERIC_FEATURES]
                x_a = features[a_idx].reshape(1, -1)
                dev_a = model_a.predict(x_a)[0]
                yield_a = trend_yield + dev_a

                # Model B prediction
                x_b = scaler.transform(features.reshape(1, -1))
                dev_b = model_b.predict(x_b)[0]
                yield_b = trend_yield + dev_b

                # Model C: analog year
                yield_c, analog_yrs = self._analog_predict(
                    features, analog_data, trend_yield, year
                )

                # Ensemble
                weights = self._get_ensemble_weights(crop, growth_stage)
                yield_ensemble = (
                    weights['model_a'] * yield_a +
                    weights['model_b'] * yield_b +
                    weights['model_c'] * yield_c
                )

                # Prediction interval
                rmse = metadata.get('rmse_cv', 10)
                confidence = self._get_confidence(week)
                width_mult = 2.5 - (confidence * 1.5)  # wider at low confidence
                yield_low = yield_ensemble - rmse * width_mult
                yield_high = yield_ensemble + rmse * width_mult

                # Last year's yield
                cur.execute("""
                    SELECT yield_per_acre FROM bronze.nass_state_yields
                    WHERE commodity = %s AND state_abbrev = %s AND year = %s
                """, (crop_db, state, year - 1))
                ly_row = cur.fetchone()
                last_year_yield = float(ly_row[0]) if ly_row and ly_row[0] else None

                # Primary driver
                primary_driver = self._identify_driver(features, ALL_NUMERIC_FEATURES)

                pred = YieldPrediction(
                    commodity=crop_db,
                    state=state,
                    year=int(year),
                    forecast_week=int(week),
                    forecast_date=forecast_date,
                    yield_forecast=float(round(yield_ensemble, 1)),
                    yield_low=float(round(yield_low, 1)),
                    yield_high=float(round(yield_high, 1)),
                    trend_yield=float(round(trend_yield, 1)),
                    vs_trend_pct=float(round((yield_ensemble - trend_yield) / trend_yield * 100, 1)) if trend_yield else 0.0,
                    last_year_yield=float(last_year_yield) if last_year_yield else None,
                    vs_last_year_pct=float(round((yield_ensemble - last_year_yield) / last_year_yield * 100, 1)) if last_year_yield else None,
                    model_type='ensemble',
                    confidence=float(round(confidence, 2)),
                    primary_driver=primary_driver,
                    analog_years=analog_yrs,
                )
                predictions.append(pred)

            return predictions

        finally:
            cur.close()
            conn.close()

    def save_predictions(self, predictions: List[YieldPrediction], run_id: str = None):
        """Save predictions to gold.yield_forecast."""
        if not predictions:
            return

        import uuid
        conn = get_db_connection()
        cur = conn.cursor()

        try:
            if run_id is None:
                run_id = str(uuid.uuid4())
                cur.execute("""
                    INSERT INTO silver.yield_model_run
                        (run_id, model_version, model_type, crops_processed,
                         forecast_week)
                    VALUES (%s, 'v1', 'ensemble', %s, %s)
                """, (run_id, predictions[0].commodity, predictions[0].forecast_week))

            for p in predictions:
                # Get previous week forecast for wow_change
                cur.execute("""
                    SELECT yield_forecast FROM gold.yield_forecast
                    WHERE commodity = %s AND state = %s AND year = %s
                      AND forecast_week = %s AND model_type = 'ensemble'
                """, (p.commodity, p.state, p.year, p.forecast_week - 1))
                prev_row = cur.fetchone()
                prev_forecast = float(prev_row[0]) if prev_row else None
                wow_change = round(p.yield_forecast - prev_forecast, 1) if prev_forecast else None

                cur.execute("""
                    INSERT INTO gold.yield_forecast
                        (run_id, commodity, state, year, forecast_week, forecast_date,
                         yield_forecast, yield_low, yield_high,
                         trend_yield, vs_trend_pct, last_year_yield, vs_last_year_pct,
                         model_type, confidence, primary_driver, analog_years,
                         prev_week_forecast, wow_change)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (commodity, state, year, forecast_week, model_type)
                    DO UPDATE SET
                        yield_forecast = EXCLUDED.yield_forecast,
                        yield_low = EXCLUDED.yield_low,
                        yield_high = EXCLUDED.yield_high,
                        trend_yield = EXCLUDED.trend_yield,
                        vs_trend_pct = EXCLUDED.vs_trend_pct,
                        confidence = EXCLUDED.confidence,
                        primary_driver = EXCLUDED.primary_driver,
                        analog_years = EXCLUDED.analog_years,
                        prev_week_forecast = EXCLUDED.prev_week_forecast,
                        wow_change = EXCLUDED.wow_change,
                        run_id = EXCLUDED.run_id,
                        created_at = NOW()
                """, (
                    run_id, p.commodity, p.state, p.year, p.forecast_week,
                    p.forecast_date,
                    p.yield_forecast, p.yield_low, p.yield_high,
                    p.trend_yield, p.vs_trend_pct, p.last_year_yield, p.vs_last_year_pct,
                    p.model_type, p.confidence, p.primary_driver, p.analog_years,
                    prev_forecast, wow_change,
                ))

            conn.commit()
            logger.info(f"Saved {len(predictions)} predictions to gold.yield_forecast")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving predictions: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # INTERNAL HELPERS
    # ------------------------------------------------------------------

    def _analog_predict(self, features: np.ndarray, analog_data: dict,
                        trend_yield: float, current_year: int) -> Tuple[float, str]:
        """Find analog years and predict yield."""
        if analog_data is None:
            return trend_yield, ''

        a_idx = [analog_data['feature_names'].index(f) for f in ANALOG_FEATURES
                 if f in analog_data['feature_names']]

        stored_features = np.array(analog_data['features'])
        stored_years = np.array(analog_data['years'])
        stored_yields = np.array(analog_data['yields'])
        stored_trends = np.array(analog_data['trend_yields'])

        # Standardize
        current_subset = features[a_idx]
        stored_subset = stored_features[:, a_idx]

        mean = np.mean(stored_subset, axis=0)
        std = np.std(stored_subset, axis=0)
        std[std == 0] = 1

        current_std = (current_subset - mean) / std
        stored_std = (stored_subset - mean) / std

        # Euclidean distances
        distances = np.sqrt(np.sum((stored_std - current_std) ** 2, axis=1))

        # Exclude current year
        mask = stored_years != current_year
        distances = distances[mask]
        years = stored_years[mask]
        yields = stored_yields[mask]
        trends = stored_trends[mask]

        if len(distances) == 0:
            return trend_yield, ''

        # Top 5 analogs
        top_idx = np.argsort(distances)[:5]
        top_years = years[top_idx]
        top_yields = yields[top_idx]
        top_trends = trends[top_idx]
        top_distances = distances[top_idx]

        # Yield deviations weighted by inverse distance
        weights = 1.0 / (top_distances + 0.01)
        weights /= weights.sum()
        deviations = top_yields - top_trends
        weighted_dev = np.sum(weights * deviations)

        yield_c = trend_yield + weighted_dev
        analog_str = "Similar to " + ", ".join([str(int(y)) for y in top_years[:3]])

        return yield_c, analog_str

    def _get_ensemble_weights(self, crop: str, growth_stage: str) -> dict:
        """Get ensemble weights for crop/growth stage."""
        crop_weights = ENSEMBLE_WEIGHTS.get(crop, ENSEMBLE_WEIGHTS.get('corn'))
        weights = crop_weights.get(growth_stage, crop_weights.get('vegetative'))
        return weights

    def _get_confidence(self, week: int) -> float:
        """Confidence score based on week of season."""
        if week in CONFIDENCE_BY_WEEK:
            return CONFIDENCE_BY_WEEK[week]
        # Interpolate
        keys = sorted(CONFIDENCE_BY_WEEK.keys())
        if week < keys[0]:
            return CONFIDENCE_BY_WEEK[keys[0]]
        if week > keys[-1]:
            return CONFIDENCE_BY_WEEK[keys[-1]]
        for i in range(len(keys) - 1):
            if keys[i] <= week <= keys[i + 1]:
                frac = (week - keys[i]) / (keys[i + 1] - keys[i])
                return CONFIDENCE_BY_WEEK[keys[i]] + frac * (
                    CONFIDENCE_BY_WEEK[keys[i + 1]] - CONFIDENCE_BY_WEEK[keys[i]]
                )
        return 0.5

    def _identify_driver(self, features: np.ndarray, feature_names: list) -> str:
        """Identify the primary driver based on feature extremes."""
        f_dict = dict(zip(feature_names, features))

        if f_dict.get('stress_days_drought', 0) > 7:
            return 'Drought stress'
        if f_dict.get('stress_days_heat', 0) > 5:
            return 'Heat stress'
        if f_dict.get('excess_moisture_days', 0) > 5:
            return 'Excess moisture'
        if f_dict.get('frost_events', 0) > 2:
            return 'Frost damage'

        ge = f_dict.get('good_excellent_pct', 0)
        if ge and ge > 70:
            return 'Strong crop conditions'
        if ge and ge < 50:
            return 'Poor crop conditions'

        precip_dev = f_dict.get('precip_vs_normal_pct', 0)
        if precip_dev and precip_dev < -25:
            return 'Below-normal precipitation'
        if precip_dev and precip_dev > 30:
            return 'Above-normal precipitation'

        return 'Normal conditions'

    def _load_pickle(self, filename: str):
        """Load a pickle file from model directory."""
        path = self.model_dir / filename
        if not path.exists():
            return None
        with open(path, 'rb') as f:
            return pickle.load(f)

    def _load_json(self, filename: str) -> dict:
        """Load a JSON file from model directory."""
        path = self.model_dir / filename
        if not path.exists():
            return {}
        with open(path) as f:
            return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="Yield Prediction Model")
    subparsers = parser.add_subparsers(dest='command')

    train_parser = subparsers.add_parser('train', help='Train models')
    train_parser.add_argument('--crop', type=str, required=True)
    train_parser.add_argument('--week', type=int, default=30, help='Target forecast week')
    train_parser.add_argument('--years', type=str, default='2005-2023')

    predict_parser = subparsers.add_parser('predict', help='Generate predictions')
    predict_parser.add_argument('--crop', type=str, required=True)
    predict_parser.add_argument('--year', type=int, required=True)
    predict_parser.add_argument('--week', type=int, required=True)
    predict_parser.add_argument('--save-db', action='store_true')

    parser.add_argument('--verbose', '-v', action='store_true')
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    model = YieldPredictionModel()

    if args.command == 'train':
        start, end = map(int, args.years.split('-'))
        result = model.train(args.crop, train_years=range(start, end + 1), target_week=args.week)
        print(f"\nTraining Results for {args.crop}:")
        print(f"  Samples: {result.get('n_samples')}")
        print(f"  RMSE:    {result.get('rmse_cv')}")
        print(f"  MAE:     {result.get('mae_cv')}")
        print(f"  R²:      {result.get('r2_cv')}")
        if 'top_features_b' in result:
            print(f"  Top features:")
            for name, imp in result['top_features_b'][:5]:
                print(f"    {name}: {imp:.3f}")

    elif args.command == 'predict':
        predictions = model.predict(args.crop, args.year, args.week)
        if predictions:
            print(f"\nPredictions for {args.crop} year={args.year} week={args.week}:")
            print(f"{'State':<6} {'Forecast':>9} {'Low':>6} {'High':>6} {'Trend':>7} {'vs Trend':>9} {'Driver'}")
            print(f"{'-'*6} {'-'*9} {'-'*6} {'-'*6} {'-'*7} {'-'*9} {'-'*20}")
            for p in sorted(predictions, key=lambda x: x.state):
                print(f"{p.state:<6} {p.yield_forecast:>9.1f} {p.yield_low:>6.1f} {p.yield_high:>6.1f} "
                      f"{p.trend_yield:>7.1f} {p.vs_trend_pct:>8.1f}% {p.primary_driver}")

            if args.save_db:
                model.save_predictions(predictions)
        else:
            print("No predictions generated")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
