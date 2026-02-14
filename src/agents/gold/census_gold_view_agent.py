"""
Census Gold View Agent
======================
Agent for creating and refreshing Gold layer views for Census trade data.

Views created:
1. gold.trade_export_matrix - Export pivot by country/month
2. gold.trade_import_matrix - Import pivot by country/month
3. gold.cottonseed_oil_trade_summary - Cottonseed oil in 000 lbs
4. gold.trade_yoy_comparison - Year-over-year analysis
5. gold.trade_commodity_summary - Commodity totals by flow

Logs all view operations for the CensusLogReaderAgent.
"""

import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from agents.base.census_base_agent import CensusBaseAgent, PipelineLayer, EventType


class CensusGoldViewAgent(CensusBaseAgent):
    """
    Agent for creating and refreshing Gold layer views.

    Creates analytics-ready views from silver layer data.
    """

    def __init__(self, **kwargs):
        super().__init__(agent_name='CensusGoldView', **kwargs)

    def get_layer(self) -> PipelineLayer:
        return PipelineLayer.GOLD

    def run(self, views_to_refresh: List[str] = None):
        """
        Create or refresh Gold layer views.

        Args:
            views_to_refresh: List of specific views to refresh (or None for all)

        Returns:
            AgentResult with view creation statistics
        """
        self.log_event(
            EventType.VERIFICATION_START,
            "Starting Gold layer view refresh"
        )

        # Define all views
        all_views = {
            'trade_export_matrix': self._create_export_matrix_view,
            'trade_import_matrix': self._create_import_matrix_view,
            'cottonseed_oil_trade_summary': self._create_cottonseed_oil_view,
            'trade_yoy_comparison': self._create_yoy_comparison_view,
            'trade_commodity_summary': self._create_commodity_summary_view,
            'soybean_complex_trade': self._create_soybean_complex_view,
            'corn_product_trade': self._create_corn_product_view,
        }

        # Determine which views to refresh
        if views_to_refresh:
            views = {k: v for k, v in all_views.items() if k in views_to_refresh}
        else:
            views = all_views

        views_created = 0
        views_failed = 0

        for view_name, create_func in views.items():
            start_time = time.time()

            try:
                create_func()
                duration_ms = int((time.time() - start_time) * 1000)

                self.log_event(
                    EventType.VIEW_CREATE,
                    f"Created/refreshed gold.{view_name}",
                    data={'view': view_name},
                    duration_ms=duration_ms
                )
                views_created += 1

            except Exception as e:
                self.log_error(
                    f"Failed to create gold.{view_name}: {e}",
                    data={'view': view_name}
                )
                views_failed += 1

        self.set_metadata('views_created', views_created)
        self.set_metadata('views_failed', views_failed)

        return self.complete(success=views_failed == 0)

    def _create_export_matrix_view(self):
        """Create export matrix view"""
        sql = """
            CREATE OR REPLACE VIEW gold.trade_export_matrix AS
            WITH monthly_exports AS (
                SELECT
                    year,
                    month,
                    commodity_group,
                    country_code,
                    country_name,
                    region,
                    display_unit,
                    SUM(quantity_display) as quantity,
                    SUM(value_usd) as value_usd
                FROM silver.census_trade_monthly
                WHERE flow = 'exports'
                  AND NOT is_regional_total
                GROUP BY year, month, commodity_group, country_code, country_name, region, display_unit
            )
            SELECT
                year,
                month,
                commodity_group,
                country_code,
                country_name,
                region,
                quantity,
                display_unit,
                value_usd,
                ROUND(value_usd / NULLIF(quantity, 0), 2) as unit_value
            FROM monthly_exports
            ORDER BY year DESC, month DESC, value_usd DESC
        """
        self.execute_query(sql, fetch=False)

    def _create_import_matrix_view(self):
        """Create import matrix view"""
        sql = """
            CREATE OR REPLACE VIEW gold.trade_import_matrix AS
            WITH monthly_imports AS (
                SELECT
                    year,
                    month,
                    commodity_group,
                    country_code,
                    country_name,
                    region,
                    display_unit,
                    SUM(quantity_display) as quantity,
                    SUM(value_usd) as value_usd
                FROM silver.census_trade_monthly
                WHERE flow = 'imports'
                  AND NOT is_regional_total
                GROUP BY year, month, commodity_group, country_code, country_name, region, display_unit
            )
            SELECT
                year,
                month,
                commodity_group,
                country_code,
                country_name,
                region,
                quantity,
                display_unit,
                value_usd,
                ROUND(value_usd / NULLIF(quantity, 0), 2) as unit_value
            FROM monthly_imports
            ORDER BY year DESC, month DESC, value_usd DESC
        """
        self.execute_query(sql, fetch=False)

    def _create_cottonseed_oil_view(self):
        """Create cottonseed oil trade summary view"""
        sql = """
            CREATE OR REPLACE VIEW gold.cottonseed_oil_trade_summary AS
            WITH cottonseed_data AS (
                SELECT
                    year,
                    month,
                    flow,
                    commodity_name,
                    country_code,
                    country_name,
                    quantity_display as quantity_000_lbs,
                    value_usd
                FROM silver.census_trade_monthly
                WHERE commodity_group = 'COTTONSEED_OIL'
                  AND NOT is_regional_total
            )
            SELECT
                year,
                month,
                flow,
                commodity_name,
                country_code,
                country_name,
                quantity_000_lbs,
                value_usd,
                '000 Pounds' as unit
            FROM cottonseed_data
            ORDER BY year DESC, month DESC, flow, value_usd DESC
        """
        self.execute_query(sql, fetch=False)

    def _create_yoy_comparison_view(self):
        """Create year-over-year comparison view"""
        sql = """
            CREATE OR REPLACE VIEW gold.trade_yoy_comparison AS
            WITH current_year AS (
                SELECT
                    EXTRACT(YEAR FROM NOW()) as current_year
            ),
            monthly_totals AS (
                SELECT
                    year,
                    month,
                    flow,
                    commodity_group,
                    display_unit,
                    SUM(quantity_display) as quantity,
                    SUM(value_usd) as value_usd
                FROM silver.census_trade_monthly
                WHERE NOT is_regional_total
                GROUP BY year, month, flow, commodity_group, display_unit
            )
            SELECT
                t.year,
                t.month,
                t.flow,
                t.commodity_group,
                t.display_unit,
                t.quantity as current_quantity,
                t.value_usd as current_value,
                py.quantity as prior_year_quantity,
                py.value_usd as prior_year_value,
                ROUND(((t.quantity - COALESCE(py.quantity, 0)) / NULLIF(py.quantity, 0)) * 100, 1) as qty_yoy_pct,
                ROUND(((t.value_usd - COALESCE(py.value_usd, 0)) / NULLIF(py.value_usd, 0)) * 100, 1) as value_yoy_pct
            FROM monthly_totals t
            LEFT JOIN monthly_totals py
                ON t.year = py.year + 1
                AND t.month = py.month
                AND t.flow = py.flow
                AND t.commodity_group = py.commodity_group
            WHERE t.year >= (SELECT current_year - 2 FROM current_year)
            ORDER BY t.year DESC, t.month DESC, t.commodity_group, t.flow
        """
        self.execute_query(sql, fetch=False)

    def _create_commodity_summary_view(self):
        """Create commodity summary view"""
        sql = """
            CREATE OR REPLACE VIEW gold.trade_commodity_summary AS
            WITH annual_totals AS (
                SELECT
                    year,
                    flow,
                    commodity_group,
                    display_unit,
                    SUM(quantity_display) as total_quantity,
                    SUM(value_usd) as total_value,
                    COUNT(DISTINCT country_code) as trading_partners
                FROM silver.census_trade_monthly
                WHERE NOT is_regional_total
                GROUP BY year, flow, commodity_group, display_unit
            )
            SELECT
                year,
                flow,
                commodity_group,
                display_unit,
                total_quantity,
                total_value,
                trading_partners,
                ROUND(total_value / NULLIF(total_quantity, 0), 2) as avg_unit_value
            FROM annual_totals
            ORDER BY year DESC, flow, total_value DESC
        """
        self.execute_query(sql, fetch=False)

    def _create_soybean_complex_view(self):
        """Create soybean complex trade view"""
        sql = """
            CREATE OR REPLACE VIEW gold.soybean_complex_trade AS
            SELECT
                year,
                month,
                flow,
                commodity_group,
                commodity_name,
                display_unit,
                SUM(quantity_display) as quantity,
                SUM(value_usd) as value_usd,
                COUNT(DISTINCT country_code) as countries
            FROM silver.census_trade_monthly
            WHERE commodity_group IN ('SOYBEANS', 'SOYBEAN_OIL', 'SOYBEAN_MEAL')
              AND NOT is_regional_total
            GROUP BY year, month, flow, commodity_group, commodity_name, display_unit
            ORDER BY year DESC, month DESC, commodity_group, flow
        """
        self.execute_query(sql, fetch=False)

    def _create_corn_product_view(self):
        """Create corn products trade view"""
        sql = """
            CREATE OR REPLACE VIEW gold.corn_product_trade AS
            SELECT
                year,
                month,
                flow,
                commodity_group,
                commodity_name,
                display_unit,
                SUM(quantity_display) as quantity,
                SUM(value_usd) as value_usd,
                COUNT(DISTINCT country_code) as countries
            FROM silver.census_trade_monthly
            WHERE commodity_group IN ('CORN', 'CORN_OIL', 'DDGS', 'ETHANOL')
              AND NOT is_regional_total
            GROUP BY year, month, flow, commodity_group, commodity_name, display_unit
            ORDER BY year DESC, month DESC, commodity_group, flow
        """
        self.execute_query(sql, fetch=False)


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for Census Gold View Agent"""
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description='Census Gold View Agent')

    parser.add_argument(
        '--views',
        nargs='+',
        help='Specific views to refresh'
    )

    args = parser.parse_args()

    agent = CensusGoldViewAgent()
    result = agent.run(views_to_refresh=args.views)

    print(f"\nResult: {'SUCCESS' if result.success else 'FAILED'}")
    print(f"Views created: {result.metadata.get('views_created', 0)}")
    print(f"Views failed: {result.metadata.get('views_failed', 0)}")
    print(f"Duration: {result.duration_seconds:.1f}s")


if __name__ == '__main__':
    main()
