"""
Bronze to Silver Trade Data Transformer

Transforms raw Census trade data from bronze layer to silver layer with:
1. Source validation (optional re-check against Census API)
2. Unit conversion (KG → Short Tons, 1000 Lbs, etc.)
3. Post-transformation verification (independent math check)
4. Logging and discrepancy tracking

Flow:
    Bronze (raw KG) → Validate → Transform → Silver (standard units) → Verify → Log

Usage:
    python -m src.agents.transformers.bronze_to_silver_trade --commodity-group SOYBEAN_MEAL_ALL
    python -m src.agents.transformers.bronze_to_silver_trade --all --verify
"""

import argparse
import logging
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env')

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONVERSION CONSTANTS
# =============================================================================
# These match the values in the reference.hs_codes table

KG_TO_MT = Decimal('0.001')                    # 1 KG = 0.001 MT
MT_TO_SHORT_TONS = Decimal('1.10231')          # 1 MT = 1.10231 short tons
KG_TO_SHORT_TONS = Decimal('0.00110231')       # KG → MT → Short tons
KG_TO_LBS = Decimal('2.20462')                 # 1 KG = 2.20462 lbs
KG_TO_1000_LBS = Decimal('0.00220462')         # KG → lbs → 1000 lbs

# Tolerance for verification (allow 0.01% difference due to rounding)
VERIFICATION_TOLERANCE = Decimal('0.0001')


@dataclass
class TransformationResult:
    """Result of a bronze→silver transformation"""
    batch_id: str
    bronze_records: int
    silver_created: int
    silver_updated: int
    verification_passed: bool
    discrepancies: int
    errors: List[str]


class BronzeToSilverTransformer:
    """
    Transforms Census trade data from bronze to silver layer.

    Process:
    1. Read bronze data for specified commodity group
    2. Look up conversion factors from reference.hs_codes
    3. Apply conversions and write to silver tables
    4. Run verification (re-calculate from bronze, compare to silver)
    5. Log results and any discrepancies
    """

    def __init__(self, conn=None):
        """Initialize with optional database connection"""
        self.conn = conn
        self._own_connection = False

        if self.conn is None:
            self._connect()
            self._own_connection = True

    def _connect(self):
        """Establish database connection"""
        password = (os.environ.get('RLC_PG_PASSWORD') or
                   os.environ.get('DATABASE_PASSWORD') or
                   os.environ.get('DB_PASSWORD'))

        self.conn = psycopg2.connect(
            host=os.environ.get('DATABASE_HOST', 'localhost'),
            port=os.environ.get('DATABASE_PORT', '5432'),
            database=os.environ.get('DATABASE_NAME', 'rlc_commodities'),
            user=os.environ.get('DATABASE_USER', 'postgres'),
            password=password
        )
        logger.info("Connected to database")

    def close(self):
        """Close database connection if we own it"""
        if self._own_connection and self.conn:
            self.conn.close()

    def get_hs_codes_for_group(self, commodity_group: str,
                               for_date: date = None) -> List[Dict]:
        """
        Get all HS codes for a commodity group, valid for the given date.

        Returns list of dicts with hs_code, conversion_factor, etc.
        """
        for_date = for_date or date.today()

        cur = self.conn.cursor()
        cur.execute("""
            SELECT hs_code, description, census_unit, standard_unit,
                   conversion_factor, commodity_subgroup
            FROM reference.hs_codes
            WHERE commodity_group = %s
              AND valid_from <= %s
              AND (valid_to IS NULL OR valid_to >= %s)
        """, (commodity_group, for_date, for_date))

        columns = ['hs_code', 'description', 'census_unit', 'standard_unit',
                   'conversion_factor', 'commodity_subgroup']

        return [dict(zip(columns, row)) for row in cur.fetchall()]

    def transform_commodity_group(
        self,
        commodity_group: str,
        start_date: date = None,
        end_date: date = None,
        verify: bool = True,
        triggered_by: str = 'MANUAL'
    ) -> TransformationResult:
        """
        Transform bronze data to silver for a commodity group.

        Args:
            commodity_group: e.g., 'SOYBEAN_MEAL_ALL'
            start_date: Optional start date filter
            end_date: Optional end date filter
            verify: Whether to run verification after transformation
            triggered_by: Who/what triggered this transformation

        Returns:
            TransformationResult with counts and status
        """
        batch_id = str(uuid.uuid4())
        started_at = datetime.now()
        errors = []

        logger.info(f"Starting transformation batch {batch_id} for {commodity_group}")

        # Get HS codes for this group
        hs_codes_info = self.get_hs_codes_for_group(commodity_group)
        if not hs_codes_info:
            logger.warning(f"No HS codes found for commodity group: {commodity_group}")
            return TransformationResult(
                batch_id=batch_id,
                bronze_records=0,
                silver_created=0,
                silver_updated=0,
                verification_passed=False,
                discrepancies=0,
                errors=[f"No HS codes found for {commodity_group}"]
            )

        hs_codes = [h['hs_code'] for h in hs_codes_info]
        hs_code_map = {h['hs_code']: h for h in hs_codes_info}

        logger.info(f"Processing HS codes: {hs_codes}")

        cur = self.conn.cursor()

        # Build date filter
        date_filter = ""
        date_params = []
        if start_date:
            date_filter += " AND (year > %s OR (year = %s AND month >= %s))"
            date_params.extend([start_date.year, start_date.year, start_date.month])
        if end_date:
            date_filter += " AND (year < %s OR (year = %s AND month <= %s))"
            date_params.extend([end_date.year, end_date.year, end_date.month])

        # Get bronze data (using TOTAL FOR ALL COUNTRIES for accurate totals)
        query = f"""
            SELECT id, year, month, flow, hs_code, quantity, value_usd
            FROM bronze.census_trade
            WHERE hs_code = ANY(%s)
              AND country_name = 'TOTAL FOR ALL COUNTRIES'
              {date_filter}
            ORDER BY year, month, flow, hs_code
        """

        cur.execute(query, [hs_codes] + date_params)
        bronze_records = cur.fetchall()

        logger.info(f"Found {len(bronze_records)} bronze records")

        silver_created = 0
        silver_updated = 0

        for record in bronze_records:
            bronze_id, year, month, flow, hs_code, quantity, value_usd = record

            if quantity is None:
                continue

            hs_info = hs_code_map.get(hs_code, {})
            conversion_factor = Decimal(str(hs_info.get('conversion_factor', '0.00110231')))
            standard_unit = hs_info.get('standard_unit', 'SHORT_TONS')
            census_unit = hs_info.get('census_unit', 'KG')

            # Convert quantity based on census_unit
            quantity_decimal = Decimal(str(quantity))

            # Calculate all unit conversions based on source unit
            if census_unit == 'T':
                # Source is already in metric tons
                quantity_mt = quantity_decimal
                quantity_short_tons = quantity_decimal * MT_TO_SHORT_TONS
                quantity_1000_lbs = quantity_decimal * Decimal('1000') * KG_TO_1000_LBS  # MT -> KG -> lbs
                conversion_formula = 'MT * 1.10231'
            else:
                # Source is in KG (default)
                quantity_mt = quantity_decimal * KG_TO_MT
                quantity_short_tons = quantity_decimal * KG_TO_SHORT_TONS
                quantity_1000_lbs = quantity_decimal * KG_TO_1000_LBS
                if standard_unit == 'SHORT_TONS':
                    conversion_formula = 'KG / 1000 * 1.10231'
                elif standard_unit == '1000_LBS':
                    conversion_formula = 'KG * 2.20462 / 1000'
                else:
                    conversion_formula = 'KG / 1000'

            # Upsert to silver table
            cur.execute("""
                INSERT INTO silver.census_trade_monthly
                    (year, month, flow, hs_code, commodity_group, commodity_description,
                     bronze_quantity, bronze_unit, bronze_value_usd,
                     quantity_mt, quantity_short_tons, quantity_1000_lbs, value_usd,
                     conversion_factor, conversion_formula, bronze_record_ids,
                     transformation_batch_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (year, month, flow, hs_code)
                DO UPDATE SET
                    bronze_quantity = EXCLUDED.bronze_quantity,
                    bronze_value_usd = EXCLUDED.bronze_value_usd,
                    quantity_mt = EXCLUDED.quantity_mt,
                    quantity_short_tons = EXCLUDED.quantity_short_tons,
                    quantity_1000_lbs = EXCLUDED.quantity_1000_lbs,
                    value_usd = EXCLUDED.value_usd,
                    bronze_record_ids = EXCLUDED.bronze_record_ids,
                    transformation_batch_id = EXCLUDED.transformation_batch_id,
                    updated_at = NOW()
                RETURNING (xmax = 0) as inserted
            """, (
                year, month, flow, hs_code, commodity_group, hs_info.get('description'),
                quantity, census_unit, value_usd,
                float(quantity_mt), float(quantity_short_tons), float(quantity_1000_lbs), value_usd,
                float(conversion_factor), conversion_formula, [bronze_id],
                batch_id
            ))

            was_insert = cur.fetchone()[0]
            if was_insert:
                silver_created += 1
            else:
                silver_updated += 1

        # Now aggregate by commodity group
        self._aggregate_commodity_group(cur, commodity_group, batch_id, start_date, end_date)

        self.conn.commit()

        # Run verification if requested
        verification_passed = True
        discrepancies = 0

        if verify:
            verification_passed, discrepancies = self._verify_transformation(
                cur, batch_id, commodity_group, start_date, end_date
            )
            self.conn.commit()

        # Log the transformation
        completed_at = datetime.now()
        cur.execute("""
            INSERT INTO silver.transformation_log
                (batch_id, table_name, transformation_type,
                 bronze_records_processed, silver_records_created, silver_records_updated,
                 transformation_verified, verification_time, verification_result,
                 verification_discrepancies,
                 started_at, completed_at, triggered_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            batch_id, 'bronze.census_trade', 'UNIT_CONVERSION',
            len(bronze_records), silver_created, silver_updated,
            verify, completed_at if verify else None,
            'PASS' if verification_passed else 'FAIL' if verify else None,
            discrepancies,
            started_at, completed_at, triggered_by
        ))

        self.conn.commit()

        logger.info(f"Transformation complete: {silver_created} created, {silver_updated} updated, "
                   f"verification={'PASS' if verification_passed else 'FAIL'}")

        return TransformationResult(
            batch_id=batch_id,
            bronze_records=len(bronze_records),
            silver_created=silver_created,
            silver_updated=silver_updated,
            verification_passed=verification_passed,
            discrepancies=discrepancies,
            errors=errors
        )

    def _aggregate_commodity_group(
        self,
        cur,
        commodity_group: str,
        batch_id: str,
        start_date: date = None,
        end_date: date = None
    ):
        """Aggregate silver records into commodity group totals"""

        # Get group name
        cur.execute("""
            SELECT group_name FROM reference.commodity_groups
            WHERE group_code = %s
        """, (commodity_group,))
        result = cur.fetchone()
        group_name = result[0] if result else commodity_group

        # Build date filter
        date_filter = ""
        date_params = [commodity_group]
        if start_date:
            date_filter += " AND (year > %s OR (year = %s AND month >= %s))"
            date_params.extend([start_date.year, start_date.year, start_date.month])
        if end_date:
            date_filter += " AND (year < %s OR (year = %s AND month <= %s))"
            date_params.extend([end_date.year, end_date.year, end_date.month])

        # Aggregate and upsert
        cur.execute(f"""
            INSERT INTO silver.trade_by_commodity_group
                (year, month, flow, commodity_group, commodity_group_name,
                 total_mt, total_short_tons, total_1000_lbs, total_value_usd,
                 hs_codes_included, num_hs_codes, transformation_batch_id,
                 created_at, updated_at)
            SELECT
                year, month, flow, commodity_group, %s,
                SUM(quantity_mt), SUM(quantity_short_tons), SUM(quantity_1000_lbs), SUM(value_usd),
                ARRAY_AGG(DISTINCT hs_code), COUNT(DISTINCT hs_code), %s,
                NOW(), NOW()
            FROM silver.census_trade_monthly
            WHERE commodity_group = %s
              {date_filter}
            GROUP BY year, month, flow, commodity_group
            ON CONFLICT (year, month, flow, commodity_group)
            DO UPDATE SET
                total_mt = EXCLUDED.total_mt,
                total_short_tons = EXCLUDED.total_short_tons,
                total_1000_lbs = EXCLUDED.total_1000_lbs,
                total_value_usd = EXCLUDED.total_value_usd,
                hs_codes_included = EXCLUDED.hs_codes_included,
                num_hs_codes = EXCLUDED.num_hs_codes,
                transformation_batch_id = EXCLUDED.transformation_batch_id,
                updated_at = NOW()
        """, [group_name, batch_id] + date_params)

        logger.info(f"Aggregated {cur.rowcount} commodity group records")

    def _verify_transformation(
        self,
        cur,
        batch_id: str,
        commodity_group: str,
        start_date: date = None,
        end_date: date = None
    ) -> Tuple[bool, int]:
        """
        Verify silver data by re-calculating from bronze and comparing.

        Returns (passed: bool, discrepancy_count: int)
        """
        logger.info("Running verification...")

        # Build date filter
        date_filter = ""
        date_params = [commodity_group]
        if start_date:
            date_filter += " AND (s.year > %s OR (s.year = %s AND s.month >= %s))"
            date_params.extend([start_date.year, start_date.year, start_date.month])
        if end_date:
            date_filter += " AND (s.year < %s OR (s.year = %s AND s.month <= %s))"
            date_params.extend([end_date.year, end_date.year, end_date.month])

        # Compare silver values to independent calculation from bronze
        # Uses bronze_unit to determine correct conversion formula
        cur.execute(f"""
            SELECT
                s.id,
                s.hs_code,
                s.year,
                s.month,
                s.flow,
                s.bronze_unit,
                s.quantity_short_tons as silver_short_tons,
                CASE
                    WHEN s.bronze_unit = 'T' THEN s.bronze_quantity * 1.10231
                    ELSE s.bronze_quantity / 1000 * 1.10231
                END as expected_short_tons,
                s.quantity_1000_lbs as silver_1000_lbs,
                CASE
                    WHEN s.bronze_unit = 'T' THEN s.bronze_quantity * 1000 * 2.20462 / 1000
                    ELSE s.bronze_quantity * 2.20462 / 1000
                END as expected_1000_lbs
            FROM silver.census_trade_monthly s
            WHERE s.commodity_group = %s
              {date_filter}
        """, date_params)

        discrepancies = 0

        for row in cur.fetchall():
            (silver_id, hs_code, year, month, flow, bronze_unit,
             silver_st, expected_st, silver_lbs, expected_lbs) = row

            # Check short tons
            if silver_st and expected_st:
                diff = abs(Decimal(str(silver_st)) - Decimal(str(expected_st)))
                if expected_st > 0:
                    pct_diff = diff / Decimal(str(expected_st))
                    if pct_diff > VERIFICATION_TOLERANCE:
                        discrepancies += 1
                        cur.execute("""
                            INSERT INTO silver.verification_discrepancies
                                (batch_id, silver_table, silver_record_id, field_name,
                                 expected_value, actual_value, difference, difference_pct,
                                 hs_code, year, month, flow)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            batch_id, 'silver.census_trade_monthly', silver_id,
                            'quantity_short_tons',
                            float(expected_st), float(silver_st), float(diff), float(pct_diff * 100),
                            hs_code, year, month, flow
                        ))

            # Check 1000 lbs
            if silver_lbs and expected_lbs:
                diff = abs(Decimal(str(silver_lbs)) - Decimal(str(expected_lbs)))
                if expected_lbs > 0:
                    pct_diff = diff / Decimal(str(expected_lbs))
                    if pct_diff > VERIFICATION_TOLERANCE:
                        discrepancies += 1
                        cur.execute("""
                            INSERT INTO silver.verification_discrepancies
                                (batch_id, silver_table, silver_record_id, field_name,
                                 expected_value, actual_value, difference, difference_pct,
                                 hs_code, year, month, flow)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            batch_id, 'silver.census_trade_monthly', silver_id,
                            'quantity_1000_lbs',
                            float(expected_lbs), float(silver_lbs), float(diff), float(pct_diff * 100),
                            hs_code, year, month, flow
                        ))

        # Update silver records as verified
        cur.execute("""
            UPDATE silver.census_trade_monthly
            SET verified = TRUE, verified_at = NOW()
            WHERE transformation_batch_id = %s
        """, (batch_id,))

        passed = discrepancies == 0
        logger.info(f"Verification {'PASSED' if passed else 'FAILED'}: {discrepancies} discrepancies")

        return passed, discrepancies


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Transform Census trade data from bronze to silver')

    parser.add_argument('--commodity-group', '-g',
                       help='Commodity group to transform (e.g., SOYBEAN_MEAL_ALL)')
    parser.add_argument('--all', action='store_true',
                       help='Transform all commodity groups')
    parser.add_argument('--verify', action='store_true', default=True,
                       help='Run verification after transformation (default: True)')
    parser.add_argument('--no-verify', action='store_true',
                       help='Skip verification')
    parser.add_argument('--start-date',
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date',
                       help='End date (YYYY-MM-DD)')

    args = parser.parse_args()

    if args.no_verify:
        args.verify = False

    start_date = None
    end_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    transformer = BronzeToSilverTransformer()

    try:
        if args.all:
            # Transform all defined commodity groups
            groups = ['SOYBEAN_MEAL_ALL', 'SOYBEAN_OIL_ALL', 'SOYBEANS_ALL']
            for group in groups:
                result = transformer.transform_commodity_group(
                    group, start_date, end_date, args.verify
                )
                print(f"{group}: {result.silver_created} created, {result.silver_updated} updated, "
                      f"verification={'PASS' if result.verification_passed else 'FAIL'}")

        elif args.commodity_group:
            result = transformer.transform_commodity_group(
                args.commodity_group, start_date, end_date, args.verify
            )
            print(f"Batch ID: {result.batch_id}")
            print(f"Bronze records: {result.bronze_records}")
            print(f"Silver created: {result.silver_created}")
            print(f"Silver updated: {result.silver_updated}")
            print(f"Verification: {'PASS' if result.verification_passed else 'FAIL'}")
            if result.discrepancies > 0:
                print(f"Discrepancies: {result.discrepancies}")

        else:
            parser.print_help()

    finally:
        transformer.close()


if __name__ == '__main__':
    main()
