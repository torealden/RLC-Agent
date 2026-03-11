"""
Auto-Actual Recording Hooks

Called after collector success to automatically record actual values
from newly collected data into the forecast tracker.

Collector → auto_actual hook → core.actuals → match to forecasts

Supported hooks:
- WASDE collector  → records USDA yield, production, ending stocks as actuals
- NASS processing  → records realized crush/oil production as actuals
- Crop conditions  → records G/E ratings as actuals
- EIA ethanol      → records production/stocks as actuals
"""

import logging
from datetime import date, datetime
from typing import List, Optional

logger = logging.getLogger(__name__)


def _get_tracker():
    from src.services.forecast.tracker import ForecastTracker
    return ForecastTracker()


def _get_connection():
    from src.services.database.db_config import get_connection
    return get_connection()


def record_wasde_actuals(run_date: date = None) -> int:
    """
    Record WASDE balance sheet values as actuals.

    When a new WASDE report arrives, the latest estimates for
    yield, production, and ending stocks become the current "actual"
    that prior forecasts should be scored against.
    """
    from src.services.forecast.tracker import Actual
    run_date = run_date or date.today()
    tracker = _get_tracker()
    count = 0

    with _get_connection() as conn:
        cur = conn.cursor()
        # Get the latest report's key estimates for US commodities
        cur.execute("""
            SELECT commodity, marketing_year, report_date,
                   yield, production, ending_stocks, exports,
                   domestic_consumption, crush
            FROM bronze.fas_psd
            WHERE country_code = 'US'
              AND report_date = (SELECT MAX(report_date) FROM bronze.fas_psd WHERE country_code = 'US')
              AND commodity IN ('corn', 'soybeans', 'wheat')
              AND ending_stocks IS NOT NULL
        """)
        rows = cur.fetchall()

    for row in rows:
        commodity, my, report_date, yld, prod, es, exp, dom, crush = row
        target_date = str(report_date)
        my_str = f"{my}/{my + 1 - 2000}" if my else None

        # Map column -> (forecast_type, value, unit)
        attrs = [
            ('yield', yld, 'MT/ha'),
            ('production', prod, '1000 MT'),
            ('ending_stocks', es, '1000 MT'),
            ('exports', exp, '1000 MT'),
        ]
        if crush:
            attrs.append(('crush', crush, '1000 MT'))

        for ftype, value, unit in attrs:
            if value is None:
                continue
            actual = Actual(
                actual_id=None,
                report_date=str(run_date),
                target_date=target_date,
                commodity=commodity,
                country='US',
                value_type=ftype,
                value=float(value),
                unit=unit,
                marketing_year=my_str,
                source='USDA_WASDE',
            )
            try:
                tracker.record_actual(actual)
                count += 1
            except Exception as e:
                logger.debug(f"Skipped WASDE actual {commodity}/{ftype}: {e}")

    if count > 0:
        matched = tracker.match_forecasts_to_actuals()
        logger.info(f"WASDE auto-actuals: {count} recorded, {matched} pairs matched")

    return count


def record_nass_processing_actuals(run_date: date = None) -> int:
    """
    Record NASS processing report values as actuals.

    Covers: soybean crush, corn for ethanol, soybean oil production.
    """
    from src.services.forecast.tracker import Actual
    run_date = run_date or date.today()
    tracker = _get_tracker()
    count = 0

    with _get_connection() as conn:
        cur = conn.cursor()
        # Get the latest month's realized values
        cur.execute("""
            SELECT commodity, attribute, calendar_year, month,
                   realized_value, source
            FROM silver.monthly_realized
            WHERE (calendar_year, month) = (
                SELECT calendar_year, month
                FROM silver.monthly_realized
                ORDER BY calendar_year DESC, month DESC
                LIMIT 1
            )
        """)
        rows = cur.fetchall()

    for row in rows:
        commodity, attribute, year, month, value, source = row
        if value is None:
            continue

        # Map NASS attribute to forecast type
        attr_map = {
            'crush': 'crush',
            'oil_production_crude': 'production',
            'oil_production_refined': 'production',
            'flour_production': 'production',
        }
        ftype = attr_map.get(attribute, attribute)
        target_date = f"{year}-{month:02d}-01"

        actual = Actual(
            actual_id=None,
            report_date=str(run_date),
            target_date=target_date,
            commodity=commodity,
            country='US',
            value_type=ftype,
            value=float(value),
            unit='lbs' if 'oil' in attribute else 'bu',
            source=f'NASS_{source}' if source else 'NASS',
        )
        try:
            tracker.record_actual(actual)
            count += 1
        except Exception as e:
            logger.debug(f"Skipped NASS actual {commodity}/{attribute}: {e}")

    if count > 0:
        tracker.match_forecasts_to_actuals()

    return count


def record_crop_condition_actuals(run_date: date = None) -> int:
    """
    Record crop condition G/E ratings as actuals.
    """
    from src.services.forecast.tracker import Actual
    run_date = run_date or date.today()
    tracker = _get_tracker()
    count = 0

    with _get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT commodity, week_ending,
                   COALESCE(excellent, 0) + COALESCE(good, 0) AS ge_pct
            FROM bronze.nass_crop_condition
            WHERE week_ending = (
                SELECT MAX(week_ending) FROM bronze.nass_crop_condition
            )
            AND state = 'US TOTAL'
        """)
        rows = cur.fetchall()

    for row in rows:
        commodity, week_ending, ge_pct = row
        if ge_pct is None:
            continue

        actual = Actual(
            actual_id=None,
            report_date=str(run_date),
            target_date=str(week_ending),
            commodity=commodity,
            country='US',
            value_type='crop_condition',
            value=float(ge_pct),
            unit='pct_ge',
            source='USDA_NASS',
        )
        try:
            tracker.record_actual(actual)
            count += 1
        except Exception as e:
            logger.debug(f"Skipped condition actual {commodity}: {e}")

    if count > 0:
        tracker.match_forecasts_to_actuals()

    return count


def run_auto_actuals(collector_name: str, run_date: date = None) -> int:
    """
    Dispatcher hook: call after a collector succeeds to record auto-actuals.

    Args:
        collector_name: Name of the collector that just ran
        run_date: Override date (default today)

    Returns:
        Number of actuals recorded
    """
    hooks = {
        'usda_wasde': record_wasde_actuals,
        'nass_processing': record_nass_processing_actuals,
        'usda_nass_crop_progress': record_crop_condition_actuals,
    }

    hook = hooks.get(collector_name)
    if hook is None:
        return 0

    try:
        return hook(run_date)
    except Exception as e:
        logger.warning(f"Auto-actual hook failed for {collector_name}: {e}")
        return 0
