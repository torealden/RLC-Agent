"""
Pace Calculator — Phase 5 Knowledge Graph Growth

Computes cumulative pace vs USDA annual projections and writes results
as 'pace_tracking' context entries on KG nodes. Answers the question:
"Are we on track to hit the USDA forecast?"

Current computations:
  1. Soybean crush pace: monthly NASS crush (short tons) vs USDA annual
     projection (1000 MT). Conversion: 1 (1000 MT) = 1102.31 short tons.
  2. Corn grind pace: monthly NASS grain crush (bushels) — year-over-year
     comparison since NASS grain crush covers only a subset of USDA FSI.

Standalone: python -m src.knowledge_graph.pace_calculator
"""

import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# Marketing year start months (month the MY begins)
MY_START = {
    'soybeans': 9,  # Sep
    'corn': 9,      # Sep
    'wheat': 6,     # Jun
}

# Unit conversions verified against actual data:
# silver.monthly_realized soy crush is in short tons (unit='TONS')
# 1 metric ton = 1.10231 short tons
# 1 (1000 MT) = 1000 * 1.10231 = 1102.31 short tons
SHORT_TONS_PER_1000MT = 1000 / 0.907185  # ≈ 1102.31


@dataclass
class PaceResult:
    """Result of a pace calculation run."""
    calculator: str
    success: bool
    commodities_computed: int = 0
    contexts_written: int = 0
    contexts_updated: int = 0
    errors: List[str] = field(default_factory=list)
    elapsed_seconds: float = 0.0


class PaceCalculator:
    """Computes cumulative pace vs USDA projections."""

    def __init__(self):
        self._kg = None

    @property
    def kg(self):
        if self._kg is None:
            from src.knowledge_graph.kg_manager import KGManager
            self._kg = KGManager()
        return self._kg

    def _get_connection(self):
        from src.services.database.db_config import get_connection
        return get_connection()

    def run_all(self) -> List[PaceResult]:
        """Run all pace calculators."""
        return [
            self.compute_soy_crush_pace(),
            self.compute_corn_grind_pace(),
        ]

    # ------------------------------------------------------------------
    # Soybean Crush Pace
    # ------------------------------------------------------------------
    def compute_soy_crush_pace(self) -> PaceResult:
        """
        Compare cumulative monthly soybean crush to USDA annual projection.

        Data sources:
          - silver.monthly_realized (NASS_SOY_CRUSH, attribute='crush', unit=TONS)
            Values in short tons.
          - bronze.fas_psd (USDA soybean crush projection, 1000 MT)

        Conversion: projection_1000mt * 1102.31 = projection in short tons
        """
        start = datetime.now()
        errors = []
        written = 0
        updated = 0

        try:
            projections = self._get_usda_crush_projections('soybeans')
            if not projections:
                errors.append("No USDA soybean crush projections found")
                return self._make_result('soy_crush_pace', 0, written, updated, errors, start)

            monthly = self._get_monthly_crush('soybeans', 'NASS_SOY_CRUSH')
            if not monthly:
                errors.append("No monthly soybean crush data found")
                return self._make_result('soy_crush_pace', 0, written, updated, errors, start)

            for my, projection_1000mt in projections.items():
                my_data = self._filter_my_months(monthly, my, MY_START['soybeans'])
                if not my_data:
                    continue

                cumulative = sum(float(m['realized_value']) for m in my_data)
                months_reported = len(my_data)
                annualized = cumulative / months_reported * 12

                # Convert projection to short tons for apples-to-apples comparison
                projection_short_tons = projection_1000mt * SHORT_TONS_PER_1000MT
                pace_pct = (annualized / projection_short_tons) * 100 if projection_short_tons > 0 else 0

                # Also express in 1000 MT for readability
                annualized_1000mt = annualized * 0.907185 / 1000
                cumulative_1000mt = cumulative * 0.907185 / 1000

                pace = {
                    'marketing_year': my,
                    'months_reported': months_reported,
                    'months_total': 12,
                    'cumulative_short_tons': round(cumulative, 0),
                    'cumulative_1000mt': round(cumulative_1000mt, 0),
                    'annualized_short_tons': round(annualized, 0),
                    'annualized_1000mt': round(annualized_1000mt, 0),
                    'usda_projection_1000mt': float(projection_1000mt),
                    'pace_pct_of_projection': round(pace_pct, 1),
                    'on_track': 90 <= pace_pct <= 110,
                    'assessment': (
                        'above_pace' if pace_pct > 105 else
                        'on_pace' if pace_pct >= 95 else
                        'below_pace'
                    ),
                    'monthly_detail': [
                        {'year': int(m['calendar_year']), 'month': int(m['month']),
                         'value_short_tons': float(m['realized_value'])}
                        for m in my_data
                    ],
                }

                result = self.kg.upsert_context(
                    node_key='soybeans',
                    context_type='pace_tracking',
                    context_key=f'soy_crush_pace_my{my}',
                    context_value={
                        'description': f'Soybean crush pace vs USDA projection, MY {my}/{my+1}',
                        'computed_at': datetime.now().isoformat(),
                        **pace,
                    },
                    applicable_when='always',
                    source='computed',
                )

                if result['action'] == 'inserted':
                    written += 1
                else:
                    updated += 1

        except Exception as e:
            errors.append(f"soy_crush: {e}")
            logger.error(f"Soy crush pace failed: {e}")

        return self._make_result('soy_crush_pace', 1 if (written + updated) > 0 else 0,
                                 written, updated, errors, start)

    # ------------------------------------------------------------------
    # Corn Grind Pace (year-over-year)
    # ------------------------------------------------------------------
    def compute_corn_grind_pace(self) -> PaceResult:
        """
        Compute corn grind year-over-year pace.

        NASS grain crush (source=NASS_GRAIN_CRUSH) covers corn processed
        for alcohol, which is a subset of total USDA FSI. Direct comparison
        to USDA FSI is invalid, so we use year-over-year pace instead.

        Data source:
          - silver.monthly_realized (NASS_GRAIN_CRUSH, attribute='crush', unit=BU)
        """
        start = datetime.now()
        errors = []
        written = 0
        updated = 0

        try:
            monthly = self._get_monthly_crush('corn', 'NASS_GRAIN_CRUSH')
            if not monthly:
                errors.append("No monthly corn grind data found")
                return self._make_result('corn_grind_pace', 0, written, updated, errors, start)

            # Find all marketing years with data
            my_totals = {}
            for my_start_month in [MY_START['corn']]:
                all_years = set()
                for m in monthly:
                    yr = int(m['calendar_year'])
                    mo = int(m['month'])
                    # Determine which MY this month belongs to
                    if mo >= my_start_month:
                        all_years.add(yr)
                    else:
                        all_years.add(yr - 1)

                for my in sorted(all_years):
                    my_data = self._filter_my_months(monthly, my, my_start_month)
                    if my_data:
                        cumulative = sum(float(m['realized_value']) for m in my_data)
                        months = len(my_data)
                        my_totals[my] = {
                            'cumulative': cumulative,
                            'months': months,
                            'annualized': cumulative / months * 12 if months > 0 else 0,
                            'data': my_data,
                        }

            # Compute YoY pace for each MY that has a prior year
            sorted_mys = sorted(my_totals.keys())
            for i, my in enumerate(sorted_mys):
                info = my_totals[my]

                # Find prior year for comparison
                prior_my = my - 1
                prior_info = my_totals.get(prior_my)

                if prior_info and prior_info['months'] == 12:
                    # Compare annualized current vs prior full year
                    pace_pct = (info['annualized'] / prior_info['cumulative']) * 100
                    comparison = 'vs_prior_year'
                    comparison_value = round(prior_info['cumulative'], 0)
                elif prior_info and prior_info['months'] > 0:
                    # Compare same-month cumulative pace
                    # Use only as many months as current MY has, from prior MY
                    prior_data = self._filter_my_months(monthly, prior_my, MY_START['corn'])
                    prior_same_months = prior_data[:info['months']]
                    if prior_same_months:
                        prior_cum = sum(float(m['realized_value']) for m in prior_same_months)
                        pace_pct = (info['cumulative'] / prior_cum) * 100 if prior_cum > 0 else 0
                        comparison = 'vs_prior_same_period'
                        comparison_value = round(prior_cum, 0)
                    else:
                        continue
                else:
                    # No prior year — skip
                    continue

                pace = {
                    'marketing_year': my,
                    'months_reported': info['months'],
                    'months_total': 12,
                    'cumulative_bu': round(info['cumulative'], 0),
                    'annualized_bu': round(info['annualized'], 0),
                    'comparison': comparison,
                    'comparison_value_bu': comparison_value,
                    'pace_pct_yoy': round(pace_pct, 1),
                    'assessment': (
                        'above_prior_year' if pace_pct > 105 else
                        'on_pace_with_prior' if pace_pct >= 95 else
                        'below_prior_year'
                    ),
                    'monthly_detail': [
                        {'year': int(m['calendar_year']), 'month': int(m['month']),
                         'value_bu': float(m['realized_value'])}
                        for m in info['data']
                    ],
                }

                result = self.kg.upsert_context(
                    node_key='corn',
                    context_type='pace_tracking',
                    context_key=f'corn_grind_pace_my{my}',
                    context_value={
                        'description': f'Corn grind (NASS grain crush) YoY pace, MY {my}/{my+1}',
                        'computed_at': datetime.now().isoformat(),
                        **pace,
                    },
                    applicable_when='always',
                    source='computed',
                )

                if result['action'] == 'inserted':
                    written += 1
                else:
                    updated += 1

        except Exception as e:
            errors.append(f"corn_grind: {e}")
            logger.error(f"Corn grind pace failed: {e}")

        return self._make_result('corn_grind_pace', 1 if (written + updated) > 0 else 0,
                                 written, updated, errors, start)

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------
    def _get_usda_crush_projections(self, commodity: str) -> Dict[int, float]:
        """Get USDA crush projections by marketing year from fas_psd."""
        sql = """
            SELECT marketing_year, crush
            FROM bronze.fas_psd
            WHERE country_code = 'US' AND commodity = %s AND crush IS NOT NULL
            ORDER BY marketing_year DESC
            LIMIT 5
        """
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, (commodity,))
            return {int(r['marketing_year']): float(r['crush']) for r in cur.fetchall()}

    def _get_monthly_crush(self, commodity: str, source: str) -> List[Dict]:
        """Get monthly crush/grind data from silver.monthly_realized."""
        sql = """
            SELECT calendar_year, month, realized_value
            FROM silver.monthly_realized
            WHERE commodity = %s AND source = %s AND attribute = 'crush'
            ORDER BY calendar_year, month
        """
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, (commodity, source))
            return [dict(r) for r in cur.fetchall()]

    def _filter_my_months(
        self, monthly: List[Dict], marketing_year: int, my_start_month: int
    ) -> List[Dict]:
        """Filter monthly data to a specific marketing year."""
        result = []
        for m in monthly:
            yr = int(m['calendar_year'])
            mo = int(m['month'])
            # MY 2024 = Sep 2024 - Aug 2025
            if (yr == marketing_year and mo >= my_start_month) or \
               (yr == marketing_year + 1 and mo < my_start_month):
                result.append(m)
        return result

    def _make_result(self, name, commodities, written, updated, errors, start) -> PaceResult:
        elapsed = (datetime.now() - start).total_seconds()
        success = commodities > 0 and len(errors) == 0
        logger.info(f"{name}: {written} new + {updated} updated, {len(errors)} errors, {elapsed:.1f}s")
        return PaceResult(
            calculator=name,
            success=success,
            commodities_computed=commodities,
            contexts_written=written,
            contexts_updated=updated,
            errors=errors,
            elapsed_seconds=round(elapsed, 1),
        )


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------
if __name__ == '__main__':
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
    )

    calc = PaceCalculator()

    if len(sys.argv) > 1:
        target = sys.argv[1]
        if target == 'soy':
            results = [calc.compute_soy_crush_pace()]
        elif target == 'corn':
            results = [calc.compute_corn_grind_pace()]
        else:
            print(f"Unknown target: {target}. Use 'soy' or 'corn'.")
            sys.exit(1)
    else:
        results = calc.run_all()

    for r in results:
        status = "OK" if r.success else "FAIL"
        print(
            f"  [{status}] {r.calculator}: {r.commodities_computed} commodities, "
            f"{r.contexts_written} new, {r.contexts_updated} updated, "
            f"{len(r.errors)} errors ({r.elapsed_seconds}s)"
        )
        for err in r.errors:
            print(f"    ERROR: {err}")
