"""
Crush Margin Calculator — pure calculation, no DB access.

Calculates the gross processing value (GPV) and crush margin for any oilseed
given prices and extraction parameters.

Standard Board Crush (soybeans):
    1 bushel (60 lbs) yields:
        ~11.1 lbs oil  (18.5% × 60)
        ~47.7 lbs meal (79.5% × 60)
        ~1.2 lbs hulls

    Oil revenue:  11.1 lbs × ZL (cents/lb) / 100 = $/bu
    Meal revenue: 47.7 lbs / 2000 × ZM ($/ton) = $/bu
    GPV = oil + meal revenue
    Margin = GPV - ZS ($/bu) - processing cost ($/bu)

Generalized for any oilseed by substituting extraction rates and units.
"""

import logging
from dataclasses import dataclass
from datetime import date
from typing import Optional

from .config import OilseedParams

logger = logging.getLogger(__name__)


@dataclass
class CrushMarginResult:
    """Result of a crush margin calculation for one oilseed in one period."""
    period: date
    oilseed_code: str

    # Input prices (standardized)
    oil_price_cents_lb: float
    meal_price_per_ton: float
    seed_price_per_unit: float

    # Revenue components per unit of seed
    oil_revenue_per_unit: float
    meal_revenue_per_unit: float
    gross_processing_value: float

    # Costs
    seed_cost_per_unit: float
    processing_cost_per_unit: float

    # Margin
    crush_margin: float
    margin_pct: float

    # Metadata
    price_sources: dict


class CrushMarginCalculator:
    """
    Calculates crush margins for any oilseed.

    All calculations are per unit of seed (bushel or short ton depending
    on the oilseed). The OilseedParams carry the conversion factors.
    """

    def calculate(
        self,
        params: OilseedParams,
        period: date,
        oil_price_cents_lb: float,
        meal_price_per_ton: float,
        seed_price_per_unit: float,
        price_sources: Optional[dict] = None,
    ) -> CrushMarginResult:
        """
        Calculate crush margin for one oilseed in one period.

        Args:
            params: Oilseed-specific extraction rates and costs
            period: Month being calculated
            oil_price_cents_lb: Oil price in cents per pound
            meal_price_per_ton: Meal price in $/short ton (2000 lbs)
            seed_price_per_unit: Seed price in $/seed_unit
            price_sources: Optional dict describing price origins

        Returns:
            CrushMarginResult with all components
        """
        lbs = params.seed_lbs_per_unit

        # Oil revenue per unit of seed
        # oil_yield_pct% of seed weight becomes oil, priced at cents/lb
        oil_lbs = lbs * (params.oil_yield_pct / 100.0)
        oil_revenue = oil_lbs * oil_price_cents_lb / 100.0  # convert cents to dollars

        # Meal revenue per unit of seed
        # meal_yield_pct% of seed weight becomes meal, priced per short ton
        meal_lbs = lbs * (params.meal_yield_pct / 100.0)
        meal_revenue = meal_lbs / 2000.0 * meal_price_per_ton

        gpv = oil_revenue + meal_revenue
        margin = gpv - seed_price_per_unit - params.processing_cost_per_unit

        margin_pct = 0.0
        if seed_price_per_unit > 0:
            margin_pct = margin / seed_price_per_unit * 100.0

        return CrushMarginResult(
            period=period,
            oilseed_code=params.oilseed_code,
            oil_price_cents_lb=oil_price_cents_lb,
            meal_price_per_ton=meal_price_per_ton,
            seed_price_per_unit=seed_price_per_unit,
            oil_revenue_per_unit=round(oil_revenue, 4),
            meal_revenue_per_unit=round(meal_revenue, 4),
            gross_processing_value=round(gpv, 4),
            seed_cost_per_unit=seed_price_per_unit,
            processing_cost_per_unit=params.processing_cost_per_unit,
            crush_margin=round(margin, 4),
            margin_pct=round(margin_pct, 4),
            price_sources=price_sources or {},
        )

    def board_crush(
        self,
        period: date,
        zl_cents_lb: float,
        zm_per_ton: float,
        zs_per_bu: float,
        processing_cost: float = 0.55,
    ) -> CrushMarginResult:
        """
        Convenience method for the standard CBOT soybean board crush.

        This is the benchmark calculation that every commodity analyst
        tracks. Useful for quick validation.

        Args:
            zl_cents_lb: Soybean oil (ZL) in cents/lb
            zm_per_ton: Soybean meal (ZM) in $/short ton
            zs_per_bu: Soybeans (ZS) in $/bushel
            processing_cost: $/bushel (default 0.55)
        """
        soy_params = OilseedParams(
            oilseed_code='soybeans',
            oilseed_name='Soybeans',
            oil_yield_pct=18.5,
            meal_yield_pct=79.5,
            hull_yield_pct=2.0,
            processing_cost_per_unit=processing_cost,
            seed_unit='bushel',
            seed_lbs_per_unit=60,
            oil_price_source='futures:ZL',
            meal_price_source='futures:ZM',
            seed_price_source='futures:ZS',
            my_start_month=9,
            has_nass_monthly=True,
        )
        return self.calculate(
            soy_params, period,
            zl_cents_lb, zm_per_ton, zs_per_bu,
            price_sources={'oil': f'ZL={zl_cents_lb}', 'meal': f'ZM={zm_per_ton}',
                           'seed': f'ZS={zs_per_bu}'},
        )
