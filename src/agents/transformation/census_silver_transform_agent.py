"""
Census Silver Transform Agent
=============================
Agent for transforming Census trade data from bronze to silver layer.

Transformations:
1. Unit conversions (KG to display units)
2. Apply commodity groupings
3. Map historical country codes
4. Calculate derived metrics
5. Aggregate by month/country

Unit Standards:
- Oils: 000 Pounds (× 0.0022046 from KG)
- Meals: Short Tons (× 0.00110231 from KG)
- Oilseeds: 1,000 MT (× 0.000001 from KG)
- Grains: Million Bushels (commodity-specific)

Logs all transformations for the CensusLogReaderAgent.
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


class CensusSilverTransformAgent(CensusBaseAgent):
    """
    Agent for transforming Census trade data from bronze to silver layer.

    Applies unit conversions, commodity groupings, and country mappings.
    """

    def __init__(self, **kwargs):
        super().__init__(agent_name='CensusSilverTransform', **kwargs)

        # Load commodity reference data
        self.commodity_reference = self._load_commodity_reference()

    def get_layer(self) -> PipelineLayer:
        return PipelineLayer.SILVER

    def _load_commodity_reference(self) -> Dict[str, Dict]:
        """Load commodity reference data from silver layer"""
        query = """
            SELECT hs_code_10, commodity_group, commodity_name, flow_type,
                   source_unit, display_unit, conversion_factor
            FROM silver.trade_commodity_reference
            WHERE is_active = TRUE
        """
        try:
            results = self.execute_query(query)

            reference = {}
            for row in results:
                key = (row[0], row[3])  # (hs_code, flow_type)
                reference[key] = {
                    'hs_code': row[0],
                    'commodity_group': row[1],
                    'commodity_name': row[2],
                    'flow_type': row[3],
                    'source_unit': row[4],
                    'display_unit': row[5],
                    'conversion_factor': float(row[6]) if row[6] else 1.0
                }

            self.log_event(
                EventType.INFO,
                f"Loaded {len(reference)} commodity reference entries"
            )

            return reference

        except Exception as e:
            self.log_warning(f"Could not load commodity reference: {e}")
            return {}

    def run(
        self,
        start_date: date = None,
        end_date: date = None,
        full_refresh: bool = False
    ):
        """
        Transform bronze trade data to silver layer.

        Args:
            start_date: Start date for incremental transform
            end_date: End date for incremental transform
            full_refresh: If True, process all data

        Returns:
            AgentResult with transformation statistics
        """
        self.log_event(
            EventType.VERIFICATION_START,
            "Starting silver layer transformation",
            data={'full_refresh': full_refresh}
        )

        start_time = time.time()

        # Step 1: Ensure silver table exists
        self._ensure_silver_table()

        # Step 2: Transform and load data
        if full_refresh:
            records_transformed = self._full_refresh_transform()
        else:
            records_transformed = self._incremental_transform(start_date, end_date)

        self.add_records_processed(records_transformed)

        # Step 3: Refresh aggregated views
        self._refresh_monthly_aggregates()

        duration_ms = int((time.time() - start_time) * 1000)

        self.log_event(
            EventType.DATA_TRANSFORM,
            f"Transformed {records_transformed} records to silver layer",
            data={'records': records_transformed},
            duration_ms=duration_ms
        )

        self.set_metadata('records_transformed', records_transformed)
        self.set_metadata('full_refresh', full_refresh)

        return self.complete()

    def _ensure_silver_table(self):
        """Ensure silver census trade table exists"""
        create_sql = """
            CREATE TABLE IF NOT EXISTS silver.census_trade_monthly (
                id SERIAL PRIMARY KEY,
                year INT NOT NULL,
                month INT NOT NULL,
                flow VARCHAR(10) NOT NULL,
                hs_code VARCHAR(10) NOT NULL,
                hs_code_6 VARCHAR(6),
                commodity_group VARCHAR(50),
                commodity_name VARCHAR(100),
                country_code VARCHAR(10) NOT NULL,
                country_name VARCHAR(100),
                region VARCHAR(50),
                is_regional_total BOOLEAN DEFAULT FALSE,

                -- Source values (from bronze)
                value_usd NUMERIC,
                quantity_source NUMERIC,
                source_unit VARCHAR(20),

                -- Converted values
                quantity_display NUMERIC,
                display_unit VARCHAR(30),
                conversion_factor NUMERIC,

                -- Metadata
                source VARCHAR(50) DEFAULT 'CENSUS_TRADE',
                transformed_at TIMESTAMPTZ DEFAULT NOW(),

                UNIQUE(year, month, flow, hs_code, country_code)
            );

            CREATE INDEX IF NOT EXISTS idx_silver_census_year_month
                ON silver.census_trade_monthly(year, month);
            CREATE INDEX IF NOT EXISTS idx_silver_census_commodity_group
                ON silver.census_trade_monthly(commodity_group);
            CREATE INDEX IF NOT EXISTS idx_silver_census_flow
                ON silver.census_trade_monthly(flow);
        """

        try:
            self.execute_query(create_sql, fetch=False)
            self.log_event(EventType.INFO, "Silver table verified/created")
        except Exception as e:
            self.log_error(f"Failed to create silver table: {e}")
            raise

    def _full_refresh_transform(self) -> int:
        """Full refresh of silver layer from bronze"""
        self.log_event(EventType.INFO, "Starting full refresh transform")

        # Truncate silver table
        self.execute_query("TRUNCATE silver.census_trade_monthly", fetch=False)

        # Transform all bronze data
        transform_sql = """
            INSERT INTO silver.census_trade_monthly (
                year, month, flow, hs_code, hs_code_6,
                commodity_group, commodity_name,
                country_code, country_name, region, is_regional_total,
                value_usd, quantity_source, source_unit,
                quantity_display, display_unit, conversion_factor,
                source, transformed_at
            )
            SELECT
                b.year,
                b.month,
                b.flow,
                b.hs_code,
                LEFT(b.hs_code, 6) as hs_code_6,
                COALESCE(cr.commodity_group, 'UNKNOWN') as commodity_group,
                COALESCE(cr.commodity_name, b.hs_code) as commodity_name,
                b.country_code,
                b.country_name,
                COALESCE(ctr.region, 'UNKNOWN') as region,
                COALESCE(ctr.is_regional_total, FALSE) as is_regional_total,
                b.value_usd,
                b.quantity as quantity_source,
                COALESCE(cr.source_unit, 'KG') as source_unit,
                CASE
                    WHEN cr.conversion_factor IS NOT NULL THEN
                        b.quantity * cr.conversion_factor
                    ELSE b.quantity
                END as quantity_display,
                COALESCE(cr.display_unit, 'KG') as display_unit,
                COALESCE(cr.conversion_factor, 1.0) as conversion_factor,
                'CENSUS_TRADE',
                NOW()
            FROM bronze.census_trade b
            LEFT JOIN silver.trade_commodity_reference cr
                ON b.hs_code = cr.hs_code_10
                AND UPPER(b.flow) = cr.flow_type
            LEFT JOIN silver.trade_country_reference ctr
                ON UPPER(b.country_name) = UPPER(ctr.country_name)
            ON CONFLICT (year, month, flow, hs_code, country_code)
            DO UPDATE SET
                hs_code_6 = EXCLUDED.hs_code_6,
                commodity_group = EXCLUDED.commodity_group,
                commodity_name = EXCLUDED.commodity_name,
                country_name = EXCLUDED.country_name,
                region = EXCLUDED.region,
                is_regional_total = EXCLUDED.is_regional_total,
                value_usd = EXCLUDED.value_usd,
                quantity_source = EXCLUDED.quantity_source,
                source_unit = EXCLUDED.source_unit,
                quantity_display = EXCLUDED.quantity_display,
                display_unit = EXCLUDED.display_unit,
                conversion_factor = EXCLUDED.conversion_factor,
                transformed_at = NOW()
        """

        self.execute_query(transform_sql, fetch=False)

        # Get count
        count_result = self.execute_query(
            "SELECT COUNT(*) FROM silver.census_trade_monthly"
        )
        record_count = count_result[0][0]

        self.add_records_inserted(record_count)

        self.log_event(
            EventType.DATA_SAVE,
            f"Full refresh complete: {record_count} records",
            data={'records': record_count}
        )

        return record_count

    def _incremental_transform(
        self,
        start_date: date = None,
        end_date: date = None
    ) -> int:
        """Incremental transform of new/updated bronze records"""
        self.log_event(EventType.INFO, "Starting incremental transform")

        # Default to last 3 months if no dates specified
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            if end_date.month <= 3:
                start_date = date(end_date.year - 1, end_date.month + 9, 1)
            else:
                start_date = date(end_date.year, end_date.month - 3, 1)

        self.log_event(
            EventType.INFO,
            f"Processing records from {start_date} to {end_date}"
        )

        transform_sql = """
            INSERT INTO silver.census_trade_monthly (
                year, month, flow, hs_code, hs_code_6,
                commodity_group, commodity_name,
                country_code, country_name, region, is_regional_total,
                value_usd, quantity_source, source_unit,
                quantity_display, display_unit, conversion_factor,
                source, transformed_at
            )
            SELECT
                b.year,
                b.month,
                b.flow,
                b.hs_code,
                LEFT(b.hs_code, 6) as hs_code_6,
                COALESCE(cr.commodity_group, 'UNKNOWN') as commodity_group,
                COALESCE(cr.commodity_name, b.hs_code) as commodity_name,
                b.country_code,
                b.country_name,
                COALESCE(ctr.region, 'UNKNOWN') as region,
                COALESCE(ctr.is_regional_total, FALSE) as is_regional_total,
                b.value_usd,
                b.quantity as quantity_source,
                COALESCE(cr.source_unit, 'KG') as source_unit,
                CASE
                    WHEN cr.conversion_factor IS NOT NULL THEN
                        b.quantity * cr.conversion_factor
                    ELSE b.quantity
                END as quantity_display,
                COALESCE(cr.display_unit, 'KG') as display_unit,
                COALESCE(cr.conversion_factor, 1.0) as conversion_factor,
                'CENSUS_TRADE',
                NOW()
            FROM bronze.census_trade b
            LEFT JOIN silver.trade_commodity_reference cr
                ON b.hs_code = cr.hs_code_10
                AND UPPER(b.flow) = cr.flow_type
            LEFT JOIN silver.trade_country_reference ctr
                ON UPPER(b.country_name) = UPPER(ctr.country_name)
            WHERE (b.year > %s OR (b.year = %s AND b.month >= %s))
              AND (b.year < %s OR (b.year = %s AND b.month <= %s))
            ON CONFLICT (year, month, flow, hs_code, country_code)
            DO UPDATE SET
                hs_code_6 = EXCLUDED.hs_code_6,
                commodity_group = EXCLUDED.commodity_group,
                commodity_name = EXCLUDED.commodity_name,
                country_name = EXCLUDED.country_name,
                region = EXCLUDED.region,
                is_regional_total = EXCLUDED.is_regional_total,
                value_usd = EXCLUDED.value_usd,
                quantity_source = EXCLUDED.quantity_source,
                source_unit = EXCLUDED.source_unit,
                quantity_display = EXCLUDED.quantity_display,
                display_unit = EXCLUDED.display_unit,
                conversion_factor = EXCLUDED.conversion_factor,
                transformed_at = NOW()
        """

        params = (
            start_date.year, start_date.year, start_date.month,
            end_date.year, end_date.year, end_date.month
        )

        cursor = self.conn.cursor()
        cursor.execute(transform_sql, params)
        record_count = cursor.rowcount
        self.conn.commit()
        cursor.close()

        self.add_records_inserted(record_count)

        self.log_event(
            EventType.DATA_SAVE,
            f"Incremental transform complete: {record_count} records",
            data={'records': record_count, 'start': str(start_date), 'end': str(end_date)}
        )

        return record_count

    def _refresh_monthly_aggregates(self):
        """Refresh monthly aggregate views"""
        self.log_event(EventType.INFO, "Refreshing monthly aggregates")

        # Create or refresh aggregate view
        aggregate_sql = """
            CREATE OR REPLACE VIEW silver.census_trade_aggregate AS
            SELECT
                year,
                month,
                flow,
                commodity_group,
                display_unit,
                SUM(value_usd) as total_value_usd,
                SUM(quantity_display) as total_quantity,
                COUNT(DISTINCT country_code) as country_count,
                COUNT(*) as record_count
            FROM silver.census_trade_monthly
            WHERE NOT is_regional_total
            GROUP BY year, month, flow, commodity_group, display_unit
        """

        try:
            self.execute_query(aggregate_sql, fetch=False)
            self.log_event(EventType.VIEW_REFRESH, "Refreshed silver.census_trade_aggregate")
        except Exception as e:
            self.log_warning(f"Could not refresh aggregate view: {e}")


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for Census Silver Transform Agent"""
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description='Census Silver Transform Agent')

    parser.add_argument(
        '--full-refresh',
        action='store_true',
        help='Full refresh of silver layer'
    )
    parser.add_argument(
        '--start-date',
        help='Start date for incremental transform (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        help='End date for incremental transform (YYYY-MM-DD)'
    )

    args = parser.parse_args()

    # Parse dates
    start_date = None
    end_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    agent = CensusSilverTransformAgent()
    result = agent.run(
        start_date=start_date,
        end_date=end_date,
        full_refresh=args.full_refresh
    )

    print(f"\nResult: {'SUCCESS' if result.success else 'FAILED'}")
    print(f"Records transformed: {result.records_processed}")
    print(f"Duration: {result.duration_seconds:.1f}s")


if __name__ == '__main__':
    main()
