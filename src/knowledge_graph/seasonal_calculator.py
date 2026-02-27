"""
Seasonal Calculator — Phase 5 Knowledge Graph Growth

Computes seasonal percentile ranges from historical data and writes
them as 'seasonal_norm' context entries on KG nodes. Designed to run:
  - After CFTC collector completes (triggered by dispatcher)
  - After crop progress collector completes (triggered by dispatcher)
  - Standalone via CLI: python -m src.knowledge_graph.seasonal_calculator

Computations:
  1. CFTC Managed Money Net: monthly percentiles (p10-p90) by commodity
  2. Crop Condition G/E: weekly percentiles by commodity (national level)

All results are idempotent — re-running produces the same output.
"""

import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# Maps CFTC commodity name (as in bronze.cftc_cot.commodity) to KG node_key
CFTC_COMMODITY_MAP = {
    'corn': 'corn',
    'soybeans': 'soybeans',
    'soybean_oil': 'soybean_oil',
    'soybean_meal': 'soybean_meal',
    'wheat_srw': 'wheat_srw',
    'wheat_hrw': 'wheat_hrw',
}

# Maps NASS crop condition commodity to KG node_key
CROP_CONDITION_MAP = {
    'corn': 'corn',
    'soybeans': 'soybeans',
    'wheat': 'wheat_srw',
}

MONTH_NAMES = {
    1: 'jan', 2: 'feb', 3: 'mar', 4: 'apr', 5: 'may', 6: 'jun',
    7: 'jul', 8: 'aug', 9: 'sep', 10: 'oct', 11: 'nov', 12: 'dec',
}


@dataclass
class CalculatorResult:
    """Result of a calculator run."""
    calculator: str
    success: bool
    commodities_computed: int = 0
    contexts_written: int = 0
    contexts_updated: int = 0
    errors: List[str] = field(default_factory=list)
    elapsed_seconds: float = 0.0


class SeasonalCalculator:
    """Computes and stores seasonal norm contexts on KG nodes."""

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

    def run_all(self) -> List[CalculatorResult]:
        """Run all seasonal calculators."""
        return [
            self.compute_cftc_seasonal_norms(),
            self.compute_crop_condition_norms(),
        ]

    # ------------------------------------------------------------------
    # CFTC Managed Money Net: Monthly Percentiles
    # ------------------------------------------------------------------
    def compute_cftc_seasonal_norms(self) -> CalculatorResult:
        """
        For each CFTC commodity, compute monthly percentiles of mm_net
        and upsert as seasonal_norm context on the commodity's KG node.
        """
        start = datetime.now()
        errors = []
        commodities_done = 0
        written = 0
        updated = 0

        # Get date range info once
        date_range = self._get_cftc_date_range()

        for cftc_name, node_key in CFTC_COMMODITY_MAP.items():
            try:
                node = self.kg.get_node(node_key)
                if node is None:
                    errors.append(f"KG node not found: {node_key}")
                    continue

                monthly = self._query_cftc_monthly_percentiles(cftc_name)
                if not monthly:
                    errors.append(f"No CFTC data for {cftc_name}")
                    continue

                context_value = {
                    'description': 'CFTC managed money net position monthly percentiles',
                    'data_start': date_range.get('min_date', ''),
                    'data_end': date_range.get('max_date', ''),
                    'total_observations': date_range.get('total', 0),
                    'computed_at': datetime.now().isoformat(),
                    'commodity': cftc_name,
                    'months': monthly,
                }

                result = self.kg.upsert_context(
                    node_key=node_key,
                    context_type='seasonal_norm',
                    context_key='cftc_mm_net_monthly',
                    context_value=context_value,
                    applicable_when='always',
                    source='computed',
                )

                if result['action'] == 'inserted':
                    written += 1
                else:
                    updated += 1
                commodities_done += 1

            except Exception as e:
                errors.append(f"{cftc_name}: {e}")
                logger.error(f"CFTC seasonal calc failed for {cftc_name}: {e}")

        elapsed = (datetime.now() - start).total_seconds()
        logger.info(
            f"CFTC seasonal norms: {commodities_done} commodities, "
            f"{written} new + {updated} updated, {len(errors)} errors, {elapsed:.1f}s"
        )

        return CalculatorResult(
            calculator='cftc_seasonal_norms',
            success=commodities_done > 0,
            commodities_computed=commodities_done,
            contexts_written=written,
            contexts_updated=updated,
            errors=errors,
            elapsed_seconds=round(elapsed, 1),
        )

    def _get_cftc_date_range(self) -> Dict[str, Any]:
        """Get the date range and total count of CFTC data."""
        sql = """
            SELECT MIN(report_date)::text AS min_date,
                   MAX(report_date)::text AS max_date,
                   COUNT(*) AS total
            FROM bronze.cftc_cot
            WHERE mm_net IS NOT NULL
        """
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            row = cur.fetchone()
            return dict(row) if row else {}

    def _query_cftc_monthly_percentiles(self, commodity: str) -> Optional[Dict]:
        """
        Compute monthly percentiles of mm_net for a commodity.

        Returns dict keyed by month name: {jan: {p10,p25,p50,p75,p90,...}, ...}
        """
        sql = """
            SELECT
                EXTRACT(MONTH FROM report_date)::int AS month_num,
                PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY mm_net)::bigint AS p10,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY mm_net)::bigint AS p25,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY mm_net)::bigint AS p50,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY mm_net)::bigint AS p75,
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY mm_net)::bigint AS p90,
                COUNT(*) AS count,
                MIN(mm_net)::bigint AS min,
                MAX(mm_net)::bigint AS max
            FROM bronze.cftc_cot
            WHERE commodity = %s
              AND mm_net IS NOT NULL
            GROUP BY EXTRACT(MONTH FROM report_date)
            ORDER BY month_num
        """
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, (commodity,))
            rows = cur.fetchall()

        if not rows:
            return None

        result = {}
        for row in rows:
            month_name = MONTH_NAMES.get(row['month_num'], str(row['month_num']))
            result[month_name] = {
                'p10': int(row['p10']),
                'p25': int(row['p25']),
                'p50': int(row['p50']),
                'p75': int(row['p75']),
                'p90': int(row['p90']),
                'count': int(row['count']),
                'min': int(row['min']),
                'max': int(row['max']),
            }

        return result

    # ------------------------------------------------------------------
    # Crop Condition G/E: Weekly Percentiles
    # ------------------------------------------------------------------
    def compute_crop_condition_norms(self) -> CalculatorResult:
        """
        For each crop commodity, compute weekly percentiles of G/E ratings
        and upsert as seasonal_norm context on the commodity's KG node.
        """
        start = datetime.now()
        errors = []
        commodities_done = 0
        written = 0
        updated = 0

        for nass_name, node_key in CROP_CONDITION_MAP.items():
            try:
                node = self.kg.get_node(node_key)
                if node is None:
                    errors.append(f"KG node not found: {node_key}")
                    continue

                weekly = self._query_crop_condition_weekly_percentiles(nass_name)
                if not weekly:
                    errors.append(f"No crop condition data for {nass_name}")
                    continue

                context_value = {
                    'description': 'Crop condition Good/Excellent weekly percentiles (national)',
                    'computed_at': datetime.now().isoformat(),
                    'commodity': nass_name,
                    'level': 'national',
                    'weeks': weekly,
                }

                result = self.kg.upsert_context(
                    node_key=node_key,
                    context_type='seasonal_norm',
                    context_key='crop_condition_ge_weekly',
                    context_value=context_value,
                    applicable_when='growing_season',
                    source='computed',
                )

                if result['action'] == 'inserted':
                    written += 1
                else:
                    updated += 1
                commodities_done += 1

            except Exception as e:
                errors.append(f"{nass_name}: {e}")
                logger.error(f"Crop condition calc failed for {nass_name}: {e}")

        elapsed = (datetime.now() - start).total_seconds()
        logger.info(
            f"Crop condition norms: {commodities_done} commodities, "
            f"{written} new + {updated} updated, {len(errors)} errors, {elapsed:.1f}s"
        )

        return CalculatorResult(
            calculator='crop_condition_norms',
            success=commodities_done > 0,
            commodities_computed=commodities_done,
            contexts_written=written,
            contexts_updated=updated,
            errors=errors,
            elapsed_seconds=round(elapsed, 1),
        )

    def _query_crop_condition_weekly_percentiles(self, commodity: str) -> Optional[Dict]:
        """
        Compute weekly percentiles of G/E ratings for a commodity.

        Returns dict keyed by week: {w18: {p10,p25,p50,p75,p90,...}, ...}
        """
        sql = """
            SELECT
                EXTRACT(WEEK FROM week_ending)::int AS week_num,
                ROUND(PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY good_excellent_pct)::numeric, 1) AS p10,
                ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY good_excellent_pct)::numeric, 1) AS p25,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY good_excellent_pct)::numeric, 1) AS p50,
                ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY good_excellent_pct)::numeric, 1) AS p75,
                ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY good_excellent_pct)::numeric, 1) AS p90,
                COUNT(*) AS count,
                ROUND(MIN(good_excellent_pct)::numeric, 1) AS min,
                ROUND(MAX(good_excellent_pct)::numeric, 1) AS max
            FROM silver.nass_crop_condition_ge
            WHERE commodity = %s
              AND state = 'US'
              AND good_excellent_pct IS NOT NULL
            GROUP BY EXTRACT(WEEK FROM week_ending)
            HAVING COUNT(*) >= 1
            ORDER BY week_num
        """
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, (commodity,))
            rows = cur.fetchall()

        if not rows:
            return None

        result = {}
        for row in rows:
            week_key = f"w{row['week_num']:02d}"
            result[week_key] = {
                'p10': float(row['p10']),
                'p25': float(row['p25']),
                'p50': float(row['p50']),
                'p75': float(row['p75']),
                'p90': float(row['p90']),
                'count': int(row['count']),
                'min': float(row['min']),
                'max': float(row['max']),
            }

        return result


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------
if __name__ == '__main__':
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
    )

    calc = SeasonalCalculator()

    if len(sys.argv) > 1:
        target = sys.argv[1]
        if target == 'cftc':
            results = [calc.compute_cftc_seasonal_norms()]
        elif target == 'crop':
            results = [calc.compute_crop_condition_norms()]
        else:
            print(f"Unknown target: {target}. Use 'cftc' or 'crop'.")
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
