"""
Volume Estimator — estimates monthly crush volumes from margins.

Two modes:
    CALIBRATED (has_nass_monthly=True):
        Regress historical monthly crush against margins + seasonal dummies
        using NASS actuals from silver.monthly_realized.
        monthly_crush = alpha + beta1*margin + beta2*margin_lag + seasonal_dummies

    UNCALIBRATED (has_nass_monthly=False):
        Spread USDA annual estimate using margin-weighted seasonal pattern.
        monthly_crush = annual_crush * seasonal_factor * margin_adjustment

Future Phase 3: plant-level agent model replaces this with bottom-up estimation.
"""

import logging
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Tuple

import numpy as np

from .config import OilseedParams

logger = logging.getLogger(__name__)

# Default soybean seasonal pattern (Jan-Dec shares of annual crush)
# Derived from historical NASS monthly crush. Peak in Oct-Mar, low Jul-Aug.
DEFAULT_SEASONAL = [
    0.088, 0.082, 0.089, 0.084, 0.086, 0.076,
    0.070, 0.073, 0.083, 0.092, 0.090, 0.087,
]


@dataclass
class VolumeEstimate:
    """Result of monthly volume estimation for one oilseed."""
    period: date
    oilseed_code: str
    estimated_crush_thou_tons: float
    estimated_oil_prod_mil_lbs: float
    estimated_meal_prod_thou_tons: float
    margin_signal: float
    capacity_util_pct: float
    seasonal_factor: float
    method: str  # 'regression', 'seasonal_spread', 'default'


class VolumeEstimator:
    """Estimates monthly crush volumes from margins and seasonal patterns."""

    # Utilization rate bounds
    MIN_UTIL = 0.30   # Floor: contract obligations keep plants at ~30% min
    MAX_UTIL = 0.95   # Ceiling: maintenance, logistics constraints
    BASE_UTIL = 0.82  # Normal operating rate

    def __init__(self):
        self._calibrations: Dict[str, dict] = {}

    def calibrate(self, oilseed_code: str, conn,
                  params: OilseedParams) -> Optional[dict]:
        """
        Calibrate the volume model using NASS monthly actuals.

        Fits: monthly_crush = alpha + beta1*margin + sum(gamma_m * month_dummy)

        Returns calibration results dict or None if insufficient data.
        """
        if not params.has_nass_monthly:
            logger.info(f"{oilseed_code}: no NASS monthly data, skipping calibration")
            return None

        cur = conn.cursor()

        # Get NASS monthly actuals
        cur.execute("""
            SELECT calendar_year, month, realized_value
            FROM silver.monthly_realized
            WHERE commodity = %s
              AND attribute = %s
              AND source = %s
            ORDER BY calendar_year, month
        """, (params.oilseed_code, params.nass_attribute, params.nass_source))

        actuals = cur.fetchall()
        if len(actuals) < 24:  # Need at least 2 years
            logger.warning(
                f"{oilseed_code}: only {len(actuals)} monthly observations, "
                f"need 24+ for calibration"
            )
            return None

        # Get corresponding margins
        cur.execute("""
            SELECT period, crush_margin
            FROM silver.oilseed_crush_margin
            WHERE oilseed_code = %s
            ORDER BY period
        """, (oilseed_code,))

        margins_by_period = {}
        for row in cur.fetchall():
            key = (row['period'].year, row['period'].month)
            margins_by_period[key] = float(row['crush_margin'])

        # Build regression arrays
        y_values = []
        x_margin = []
        x_month = []

        for row in actuals:
            yr, mo = row['calendar_year'], row['month']
            key = (yr, mo)
            if key not in margins_by_period:
                continue
            y_values.append(float(row['realized_value']))
            x_margin.append(margins_by_period[key])
            x_month.append(mo)

        if len(y_values) < 12:
            logger.warning(f"{oilseed_code}: only {len(y_values)} matched observations")
            return None

        y = np.array(y_values)
        n = len(y)

        # Build design matrix: intercept + margin + 11 month dummies
        X = np.ones((n, 13))
        X[:, 1] = np.array(x_margin)
        for i, mo in enumerate(x_month):
            if mo > 1:  # January is the reference month
                X[i, mo] = 1.0

        # OLS regression
        try:
            beta = np.linalg.lstsq(X, y, rcond=None)[0]
        except np.linalg.LinAlgError:
            logger.error(f"{oilseed_code}: regression failed")
            return None

        # Calculate R-squared
        y_hat = X @ beta
        ss_res = np.sum((y - y_hat) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0

        # Extract seasonal pattern from dummies
        seasonal = np.zeros(12)
        seasonal[0] = beta[0]  # January = intercept
        for mo in range(1, 12):
            seasonal[mo] = beta[0] + beta[mo + 1]
        # Normalize to shares
        total = seasonal.sum()
        if total > 0:
            seasonal = seasonal / total

        calibration = {
            'intercept': float(beta[0]),
            'margin_coeff': float(beta[1]),
            'month_coeffs': {mo + 1: float(beta[mo + 1]) for mo in range(1, 12)},
            'seasonal_pattern': seasonal.tolist(),
            'r_squared': float(r_squared),
            'n_obs': n,
            'mean_actual': float(np.mean(y)),
            'mape': float(np.mean(np.abs((y - y_hat) / y)) * 100) if np.all(y > 0) else None,
        }

        self._calibrations[oilseed_code] = calibration
        logger.info(
            f"{oilseed_code}: calibrated with R²={r_squared:.3f}, "
            f"MAPE={calibration['mape']:.1f}%, n={n}"
        )
        return calibration

    def estimate(
        self,
        params: OilseedParams,
        period: date,
        crush_margin: float,
        margin_history: Optional[List[float]] = None,
    ) -> VolumeEstimate:
        """
        Estimate monthly crush volume.

        Args:
            params: Oilseed parameters
            period: Month to estimate
            crush_margin: Current month's crush margin
            margin_history: Recent margin history for normalization
        """
        month = period.month
        code = params.oilseed_code

        # Determine method
        if code in self._calibrations:
            return self._estimate_regression(params, period, crush_margin)
        elif params.has_nass_monthly and params.seasonal_pattern:
            return self._estimate_seasonal(params, period, crush_margin, margin_history)
        elif params.usda_annual_crush:
            return self._estimate_annual_spread(params, period, crush_margin, margin_history)
        else:
            return self._estimate_default(params, period, crush_margin)

    def _estimate_regression(self, params: OilseedParams, period: date,
                             margin: float) -> VolumeEstimate:
        """Use calibrated regression model."""
        cal = self._calibrations[params.oilseed_code]
        month = period.month

        predicted = cal['intercept'] + cal['margin_coeff'] * margin
        if month > 1 and month in cal['month_coeffs']:
            predicted += cal['month_coeffs'][month]

        predicted = max(predicted, 0)
        seasonal = cal['seasonal_pattern'][month - 1]
        capacity = params.capacity_annual_thou_tons or 1
        util = min(max(predicted / (capacity / 12), self.MIN_UTIL), self.MAX_UTIL)

        return VolumeEstimate(
            period=period,
            oilseed_code=params.oilseed_code,
            estimated_crush_thou_tons=round(predicted, 2),
            estimated_oil_prod_mil_lbs=round(
                predicted * 2 * (params.oil_yield_pct / 100) * 1000, 2  # thou tons → mil lbs
            ),
            estimated_meal_prod_thou_tons=round(
                predicted * (params.meal_yield_pct / 100), 2
            ),
            margin_signal=round(margin, 4),
            capacity_util_pct=round(util * 100, 2),
            seasonal_factor=round(seasonal, 4),
            method='regression',
        )

    def _estimate_annual_spread(self, params: OilseedParams, period: date,
                                margin: float,
                                margin_history: Optional[List[float]]) -> VolumeEstimate:
        """Spread USDA annual estimate across months using seasonal + margin weights."""
        month = period.month
        seasonal = (params.seasonal_pattern or DEFAULT_SEASONAL)[month - 1]

        # Margin adjustment: if margin is above trailing avg, increase share; below, decrease
        margin_adj = 1.0
        if margin_history and len(margin_history) >= 6:
            avg_margin = sum(margin_history) / len(margin_history)
            if avg_margin != 0:
                z = (margin - avg_margin) / (abs(avg_margin) + 0.01)
                margin_adj = 1.0 + 0.1 * max(min(z, 2.0), -2.0)  # ±20% max

        annual = params.usda_annual_crush or 0
        monthly = annual * seasonal * margin_adj

        capacity = params.capacity_annual_thou_tons or 1
        util = min(max(monthly / (capacity / 12), self.MIN_UTIL), self.MAX_UTIL)

        return VolumeEstimate(
            period=period,
            oilseed_code=params.oilseed_code,
            estimated_crush_thou_tons=round(monthly, 2),
            estimated_oil_prod_mil_lbs=round(
                monthly * 2 * (params.oil_yield_pct / 100) * 1000, 2
            ),
            estimated_meal_prod_thou_tons=round(
                monthly * (params.meal_yield_pct / 100), 2
            ),
            margin_signal=round(margin, 4),
            capacity_util_pct=round(util * 100, 2),
            seasonal_factor=round(seasonal, 4),
            method='seasonal_spread',
        )

    def _estimate_seasonal(self, params, period, margin, margin_history):
        """Same as annual spread but using calibrated seasonal from NASS data."""
        return self._estimate_annual_spread(params, period, margin, margin_history)

    def _estimate_default(self, params, period, margin):
        """Fallback: capacity × base utilization × seasonal."""
        month = period.month
        seasonal = DEFAULT_SEASONAL[month - 1]
        capacity = params.capacity_annual_thou_tons or 100
        monthly = capacity / 12 * self.BASE_UTIL * (seasonal / (1 / 12))

        return VolumeEstimate(
            period=period,
            oilseed_code=params.oilseed_code,
            estimated_crush_thou_tons=round(monthly, 2),
            estimated_oil_prod_mil_lbs=round(
                monthly * 2 * (params.oil_yield_pct / 100) * 1000, 2
            ),
            estimated_meal_prod_thou_tons=round(
                monthly * (params.meal_yield_pct / 100), 2
            ),
            margin_signal=round(margin, 4),
            capacity_util_pct=round(self.BASE_UTIL * 100, 2),
            seasonal_factor=round(seasonal, 4),
            method='default',
        )
