"""
Oilseed Crush Engine — orchestrates margin calculation, volume estimation,
and validation for all US oilseed crushing operations.

Usage:
    python -m src.engines.oilseed_crush.engine --period 2025-01
    python -m src.engines.oilseed_crush.engine --range 2024-01 2025-12
    python -m src.engines.oilseed_crush.engine --calibrate soybeans
    python -m src.engines.oilseed_crush.engine --validate
    python -m src.engines.oilseed_crush.engine --board-crush  # quick soybean check
"""

import argparse
import json
import logging
import sys
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from src.services.database.db_config import get_connection
from .config import OilseedParams, load_config, seed_reference_table
from .margin_calculator import CrushMarginCalculator, CrushMarginResult
from .price_resolver import PriceResolver
from .volume_estimator import VolumeEstimator, VolumeEstimate

logger = logging.getLogger(__name__)


class OilseedCrushEngine:
    """
    Main engine: calculates crush margins and estimates volumes for all oilseeds.
    """

    def __init__(self, config_path: Path = None):
        self.params = load_config(config_path)
        self.margin_calc = CrushMarginCalculator()
        self.volume_est = VolumeEstimator()

    def _get_conn(self):
        """Get a raw psycopg2 connection (caller must close)."""
        import psycopg2
        import psycopg2.extras
        import os
        return psycopg2.connect(
            host=os.getenv('RLC_PG_HOST', 'localhost'),
            port=5432,
            dbname='rlc_commodities',
            user='postgres',
            password=os.getenv('RLC_PG_PASSWORD', os.getenv('DB_PASSWORD', '')),
            cursor_factory=psycopg2.extras.RealDictCursor,
        )

    def setup(self):
        """Create schema and seed reference table."""
        schema_path = Path(__file__).resolve().parents[3] / "database" / "schemas" / "036_oilseed_crush_engine.sql"
        if schema_path.exists():
            conn = self._get_conn()
            try:
                cur = conn.cursor()
                cur.execute(schema_path.read_text())
                conn.commit()
                logger.info("Schema created/updated")
                seed_reference_table(conn, self.params)
            finally:
                conn.close()
        else:
            logger.warning(f"Schema file not found: {schema_path}")

    def calculate_margin(
        self, oilseed_code: str, period: date, conn=None
    ) -> Optional[CrushMarginResult]:
        """Calculate crush margin for one oilseed in one month."""
        params = self.params.get(oilseed_code)
        if not params:
            logger.error(f"Unknown oilseed: {oilseed_code}")
            return None

        own_conn = conn is None
        if own_conn:
            conn = self._get_conn()

        try:
            resolver = PriceResolver(conn)
            oil_price, oil_desc = resolver.resolve(params.oil_price_source, period)
            meal_price, meal_desc = resolver.resolve(params.meal_price_source, period)
            seed_price, seed_desc = resolver.resolve(params.seed_price_source, period)
        finally:
            if own_conn:
                conn.close()

        # Apply unit conversion (e.g., ZS cents/bu → $/bu)
        if seed_price is not None and params.seed_price_divisor != 1.0:
            seed_price = seed_price / params.seed_price_divisor

        if oil_price is None or meal_price is None or seed_price is None:
            missing = []
            if oil_price is None: missing.append(f"oil ({oil_desc})")
            if meal_price is None: missing.append(f"meal ({meal_desc})")
            if seed_price is None: missing.append(f"seed ({seed_desc})")
            logger.warning(f"{oilseed_code} {period}: missing prices: {', '.join(missing)}")
            return None

        result = self.margin_calc.calculate(
            params, period,
            oil_price_cents_lb=oil_price,
            meal_price_per_ton=meal_price,
            seed_price_per_unit=seed_price,
            price_sources={'oil': oil_desc, 'meal': meal_desc, 'seed': seed_desc},
        )
        return result

    def save_margin(self, result: CrushMarginResult, conn=None):
        """Save a margin calculation to the database."""
        own_conn = conn is None
        if own_conn:
            conn = self._get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO silver.oilseed_crush_margin (
                    period, oilseed_code,
                    oil_price_cents_lb, meal_price_per_ton, seed_price_per_unit,
                    oil_revenue_per_unit, meal_revenue_per_unit, gross_processing_value,
                    seed_cost_per_unit, processing_cost_per_unit,
                    crush_margin, margin_pct, price_sources
                ) VALUES (
                    %(period)s, %(oilseed_code)s,
                    %(oil_price_cents_lb)s, %(meal_price_per_ton)s, %(seed_price_per_unit)s,
                    %(oil_revenue_per_unit)s, %(meal_revenue_per_unit)s, %(gross_processing_value)s,
                    %(seed_cost_per_unit)s, %(processing_cost_per_unit)s,
                    %(crush_margin)s, %(margin_pct)s, %(price_sources)s
                )
                ON CONFLICT (period, oilseed_code) DO UPDATE SET
                    oil_price_cents_lb = EXCLUDED.oil_price_cents_lb,
                    meal_price_per_ton = EXCLUDED.meal_price_per_ton,
                    seed_price_per_unit = EXCLUDED.seed_price_per_unit,
                    oil_revenue_per_unit = EXCLUDED.oil_revenue_per_unit,
                    meal_revenue_per_unit = EXCLUDED.meal_revenue_per_unit,
                    gross_processing_value = EXCLUDED.gross_processing_value,
                    seed_cost_per_unit = EXCLUDED.seed_cost_per_unit,
                    processing_cost_per_unit = EXCLUDED.processing_cost_per_unit,
                    crush_margin = EXCLUDED.crush_margin,
                    margin_pct = EXCLUDED.margin_pct,
                    price_sources = EXCLUDED.price_sources,
                    run_date = NOW()
            """, {
                'period': result.period,
                'oilseed_code': result.oilseed_code,
                'oil_price_cents_lb': result.oil_price_cents_lb,
                'meal_price_per_ton': result.meal_price_per_ton,
                'seed_price_per_unit': result.seed_price_per_unit,
                'oil_revenue_per_unit': result.oil_revenue_per_unit,
                'meal_revenue_per_unit': result.meal_revenue_per_unit,
                'gross_processing_value': result.gross_processing_value,
                'seed_cost_per_unit': result.seed_cost_per_unit,
                'processing_cost_per_unit': result.processing_cost_per_unit,
                'crush_margin': result.crush_margin,
                'margin_pct': result.margin_pct,
                'price_sources': json.dumps(result.price_sources),
            })
            conn.commit()
        finally:
            if own_conn:
                conn.close()

    def run(
        self,
        start_period: date,
        end_period: date,
        oilseeds: Optional[List[str]] = None,
        save: bool = True,
    ) -> Dict[str, List[CrushMarginResult]]:
        """
        Run margin calculations for a date range.

        Args:
            start_period: First month (date with day=1)
            end_period: Last month
            oilseeds: List of oilseed codes (default: all)
            save: Whether to persist results to database
        """
        codes = oilseeds or list(self.params.keys())
        results = {code: [] for code in codes}

        conn = self._get_conn()
        try:
            current = start_period.replace(day=1)
            end = end_period.replace(day=1)

            while current <= end:
                for code in codes:
                    result = self.calculate_margin(code, current, conn=conn)
                    if result:
                        results[code].append(result)
                        if save:
                            self.save_margin(result, conn=conn)
                        logger.info(
                            f"{code} {current.strftime('%Y-%m')}: "
                            f"GPV=${result.gross_processing_value:.2f} "
                            f"Margin=${result.crush_margin:.2f} "
                            f"({result.margin_pct:+.1f}%)"
                        )

                # Next month
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)
        finally:
            conn.close()

        return results

    def calibrate(self, oilseed_code: str) -> Optional[dict]:
        """Calibrate volume model for one oilseed using NASS data."""
        params = self.params.get(oilseed_code)
        if not params:
            logger.error(f"Unknown oilseed: {oilseed_code}")
            return None

        conn = self._get_conn()
        try:
            result = self.volume_est.calibrate(oilseed_code, conn, params)
            if result:
                cur = conn.cursor()
                cur.execute("""
                    UPDATE reference.oilseed_crush_params
                    SET reg_intercept = %s,
                        reg_margin_coeff = %s,
                        reg_r_squared = %s,
                        seasonal_pattern = %s,
                        reg_calibrated_at = NOW()
                    WHERE oilseed_code = %s
                """, (
                    result['intercept'],
                    result['margin_coeff'],
                    result['r_squared'],
                    result['seasonal_pattern'],
                    oilseed_code,
                ))
                conn.commit()
                logger.info(f"Saved calibration for {oilseed_code}")
            return result
        finally:
            conn.close()

    def validate(self, oilseed_code: str = None):
        """Print validation summary for calibrated oilseeds."""
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            if oilseed_code:
                cur.execute("""
                    SELECT * FROM gold.oilseed_crush_model_accuracy
                    WHERE oilseed_code = %s
                """, (oilseed_code,))
            else:
                cur.execute("SELECT * FROM gold.oilseed_crush_model_accuracy")

            rows = cur.fetchall()
            if not rows:
                print("No validation data available. Run --calibrate first.")
                return

            print(f"\n{'Oilseed':<15} {'N':>4} {'MAPE':>7} {'Bias':>7} {'StdDev':>7}")
            print("-" * 45)
            for row in rows:
                print(
                    f"{row['oilseed_code']:<15} "
                    f"{row['n_months']:>4} "
                    f"{row['mape']:>6.1f}% "
                    f"{row['mean_bias_pct']:>+6.1f}% "
                    f"{row['error_std_pct']:>6.1f}%"
                )
        finally:
            conn.close()

    def board_crush_check(self):
        """Quick validation: calculate today's soybean board crush from latest prices."""
        today = date.today().replace(day=1)
        result = self.calculate_margin('soybeans', today)

        if not result:
            # Try prior month
            if today.month == 1:
                prior = today.replace(year=today.year - 1, month=12)
            else:
                prior = today.replace(month=today.month - 1)
            result = self.calculate_margin('soybeans', prior)

        if result:
            print(f"\n{'='*50}")
            print(f"  SOYBEAN BOARD CRUSH — {result.period.strftime('%B %Y')}")
            print(f"{'='*50}")
            print(f"  Oil (ZL):  {result.oil_price_cents_lb:.2f} ¢/lb")
            print(f"  Meal (ZM): ${result.meal_price_per_ton:.2f}/ton")
            print(f"  Beans (ZS): ${result.seed_price_per_unit:.2f}/bu")
            print(f"{'─'*50}")
            print(f"  Oil revenue:  ${result.oil_revenue_per_unit:.4f}/bu")
            print(f"  Meal revenue: ${result.meal_revenue_per_unit:.4f}/bu")
            print(f"  GPV:          ${result.gross_processing_value:.4f}/bu")
            print(f"  Seed cost:    ${result.seed_cost_per_unit:.4f}/bu")
            print(f"  Processing:   ${result.processing_cost_per_unit:.4f}/bu")
            print(f"{'─'*50}")
            print(f"  CRUSH MARGIN: ${result.crush_margin:.4f}/bu")
            print(f"  Margin %:     {result.margin_pct:+.1f}%")
            print(f"{'='*50}")
        else:
            print("Could not calculate board crush — no price data available.")


def main():
    load_dotenv(Path(__file__).resolve().parents[3] / '.env')

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="Oilseed Crush Margin Engine")
    parser.add_argument("--setup", action="store_true", help="Create schema and seed reference table")
    parser.add_argument("--period", help="Calculate margins for one month (YYYY-MM)")
    parser.add_argument("--range", nargs=2, metavar=("START", "END"), help="Calculate for date range")
    parser.add_argument("--oilseed", help="Run for specific oilseed only")
    parser.add_argument("--calibrate", metavar="OILSEED", help="Calibrate volume model for oilseed")
    parser.add_argument("--validate", action="store_true", help="Show validation summary")
    parser.add_argument("--board-crush", action="store_true", help="Quick soybean board crush check")
    parser.add_argument("--no-save", action="store_true", help="Don't persist results")

    args = parser.parse_args()
    engine = OilseedCrushEngine()

    if args.setup:
        engine.setup()

    elif args.board_crush:
        engine.board_crush_check()

    elif args.calibrate:
        result = engine.calibrate(args.calibrate)
        if result:
            print(f"\nCalibration for {args.calibrate}:")
            print(f"  R²:    {result['r_squared']:.3f}")
            print(f"  MAPE:  {result['mape']:.1f}%")
            print(f"  N:     {result['n_obs']}")
            print(f"  β₀:    {result['intercept']:.2f}")
            print(f"  β₁:    {result['margin_coeff']:.4f}")

    elif args.validate:
        engine.validate()

    elif args.period:
        period = date.fromisoformat(args.period + "-01")
        oilseeds = [args.oilseed] if args.oilseed else None
        engine.run(period, period, oilseeds=oilseeds, save=not args.no_save)

    elif args.range:
        start = date.fromisoformat(args.range[0] + "-01")
        end = date.fromisoformat(args.range[1] + "-01")
        oilseeds = [args.oilseed] if args.oilseed else None
        engine.run(start, end, oilseeds=oilseeds, save=not args.no_save)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
