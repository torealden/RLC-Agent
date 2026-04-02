"""
DOC — Daily Operations Cycle
=============================
Runs after market close (~5:30 PM ET) to:

1. VERIFY  — Confirm all daily collectors ran successfully
2. COMPUTE — Run crush margins and feedstock allocation with closing prices
3. DETECT  — Flag price anomalies, positioning extremes, data gaps
4. ASSESS  — Generate daily internal summary
5. ALERT   — Flag anything that needs analyst attention in the morning

Usage:
    python -m src.engines.doc.daily_ops              # Full cycle
    python -m src.engines.doc.daily_ops --verify     # Just check collectors
    python -m src.engines.doc.daily_ops --compute    # Just run models
    python -m src.engines.doc.daily_ops --detect     # Just anomaly detection
    python -m src.engines.doc.daily_ops --summary    # Print today's summary
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DOC] %(message)s",
)
logger = logging.getLogger("doc")


def get_conn():
    """Get database connection."""
    import psycopg2
    import psycopg2.extras
    return psycopg2.connect(
        host=os.getenv('RLC_PG_HOST', 'localhost'),
        port=5432,
        dbname='rlc_commodities',
        user='postgres',
        password=os.getenv('RLC_PG_PASSWORD', os.getenv('DB_PASSWORD', '')),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


# ═══════════════════════════════════════════════════════════════════
# STEP 1: VERIFY — Did all daily collectors run?
# ═══════════════════════════════════════════════════════════════════

DAILY_COLLECTORS = [
    ('yfinance_futures', 'Futures prices', 18),     # Should run by 6 PM
    ('usda_ams_cash_prices', 'Cash prices', 18),    # Should run by 6 PM
    ('cme_settlements', 'CME settlements', 18),     # Should run by 6 PM
    ('weather_daily_summary', 'Weather', 12),       # Should run by noon
]

WEEKLY_COLLECTORS = {
    0: [('usda_nass_crop_progress', 'Crop Progress', 20)],   # Monday
    2: [('eia_petroleum', 'EIA Petroleum', 14),               # Wednesday
        ('eia_ethanol', 'EIA Ethanol', 14)],
    3: [('usda_fas_export_sales', 'Export Sales', 12),        # Thursday
        ('drought_monitor', 'Drought Monitor', 18)],
    4: [('cftc_cot', 'CFTC COT', 20)],                       # Friday
}


def verify_collectors(today: date) -> Dict:
    """Check that expected collectors ran today."""
    conn = get_conn()
    cur = conn.cursor()

    results = {'ok': [], 'missing': [], 'failed': []}

    # Check daily collectors
    for name, display, expected_hour in DAILY_COLLECTORS:
        cur.execute("""
            SELECT status, rows_collected, run_finished_at
            FROM core.collection_status
            WHERE collector_name = %s
              AND run_started_at::date = %s
            ORDER BY run_finished_at DESC LIMIT 1
        """, (name, today))

        row = cur.fetchone()
        if not row:
            results['missing'].append(f"{display} ({name}) — did not run")
        elif row['status'] not in ('success', 'partial'):
            results['failed'].append(f"{display} ({name}) — {row['status']}")
        else:
            results['ok'].append(f"{display}: {row['rows_collected']} rows")

    # Check weekly collectors for today's day of week
    day_of_week = today.weekday()
    for name, display, expected_hour in WEEKLY_COLLECTORS.get(day_of_week, []):
        cur.execute("""
            SELECT status, rows_collected, run_finished_at
            FROM core.collection_status
            WHERE collector_name = %s
              AND run_started_at::date = %s
            ORDER BY run_finished_at DESC LIMIT 1
        """, (name, today))

        row = cur.fetchone()
        if not row:
            results['missing'].append(f"{display} ({name}) — expected today, did not run")
        elif row['status'] not in ('success', 'partial'):
            results['failed'].append(f"{display} ({name}) — {row['status']}")
        else:
            results['ok'].append(f"{display}: {row['rows_collected']} rows")

    conn.close()
    return results


# ═══════════════════════════════════════════════════════════════════
# STEP 2: COMPUTE — Run models with today's closing prices
# ═══════════════════════════════════════════════════════════════════

def compute_crush_margins(today: date) -> Dict:
    """Run crush margin engine for current month."""
    try:
        from src.engines.oilseed_crush.engine import OilseedCrushEngine
        engine = OilseedCrushEngine()
        period = today.replace(day=1)

        results = {}
        for oilseed in ['soybeans']:  # Start with soybeans, expand later
            margin = engine.calculate_margin(oilseed, period)
            if margin:
                results[oilseed] = {
                    'margin': margin.crush_margin,
                    'margin_pct': margin.margin_pct,
                    'gpv': margin.gross_processing_value,
                    'oil_price': margin.oil_price_cents_lb,
                    'meal_price': margin.meal_price_per_ton,
                    'seed_price': margin.seed_price_per_unit,
                }
                engine.save_margin(margin)
                logger.info(f"Crush margin {oilseed}: ${margin.crush_margin:.2f}/bu ({margin.margin_pct:+.1f}%)")

        return results
    except Exception as e:
        logger.error(f"Crush margin computation failed: {e}")
        return {'error': str(e)}


def compute_feedstock_allocation(today: date) -> Dict:
    """Run feedstock allocation engine with current prices."""
    try:
        from src.engines.feedstock_allocation.allocator import FeedstockAllocator
        allocator = FeedstockAllocator()

        period = today.replace(day=1)
        result = allocator.allocate_month(period)

        if result:
            logger.info(f"Feedstock allocation complete for {period}")
            return {'status': 'complete', 'period': str(period)}
        else:
            logger.warning("Feedstock allocation returned no result")
            return {'status': 'no_result'}
    except ImportError:
        logger.info("Feedstock allocation engine not fully configured — skipping")
        return {'status': 'skipped', 'reason': 'not configured'}
    except Exception as e:
        logger.error(f"Feedstock allocation failed: {e}")
        return {'error': str(e)}


# ═══════════════════════════════════════════════════════════════════
# STEP 3: DETECT — Anomaly detection
# ═══════════════════════════════════════════════════════════════════

def detect_anomalies(today: date) -> Dict:
    """Flag unusual price moves and data issues."""
    conn = get_conn()
    cur = conn.cursor()

    anomalies = []

    # Check for large daily price moves in futures
    cur.execute("""
        WITH recent AS (
            SELECT symbol, trade_date, settlement,
                   LAG(settlement) OVER (PARTITION BY symbol ORDER BY trade_date) as prev_settle
            FROM silver.futures_price
            WHERE trade_date >= %s - INTERVAL '30 days'
        ),
        daily_changes AS (
            SELECT symbol, trade_date, settlement, prev_settle,
                   (settlement - prev_settle) as daily_change,
                   ABS(settlement - prev_settle) / NULLIF(prev_settle, 0) * 100 as pct_change
            FROM recent
            WHERE prev_settle IS NOT NULL
        ),
        stats AS (
            SELECT symbol,
                   AVG(ABS(daily_change)) as avg_abs_change,
                   STDDEV(daily_change) as std_change
            FROM daily_changes
            GROUP BY symbol
        )
        SELECT d.symbol, d.trade_date, d.settlement, d.daily_change, d.pct_change,
               s.avg_abs_change, s.std_change,
               ABS(d.daily_change) / NULLIF(s.std_change, 0) as z_score
        FROM daily_changes d
        JOIN stats s ON d.symbol = s.symbol
        WHERE d.trade_date = %s
          AND ABS(d.daily_change) / NULLIF(s.std_change, 0) > 2.0
        ORDER BY ABS(d.daily_change) / NULLIF(s.std_change, 0) DESC
    """, (today, today))

    for row in cur.fetchall():
        anomalies.append({
            'type': 'price_move',
            'symbol': row['symbol'],
            'change': float(row['daily_change']),
            'pct_change': float(row['pct_change']),
            'z_score': float(row['z_score']),
            'settlement': float(row['settlement']),
        })
        logger.warning(
            f"ANOMALY: {row['symbol']} moved {row['daily_change']:.2f} "
            f"({row['pct_change']:.1f}%, z={row['z_score']:.1f})"
        )

    # Check for data gaps — any daily collector with 0 rows today
    cur.execute("""
        SELECT collector_name, rows_collected, status
        FROM core.collection_status
        WHERE run_started_at::date = %s
          AND rows_collected = 0
          AND status = 'success'
    """, (today,))

    for row in cur.fetchall():
        anomalies.append({
            'type': 'data_gap',
            'collector': row['collector_name'],
            'detail': 'Ran successfully but returned 0 rows',
        })
        logger.warning(f"DATA GAP: {row['collector_name']} returned 0 rows")

    # Check CFTC positioning extremes (if Friday)
    if today.weekday() == 4:
        cur.execute("""
            SELECT commodity, report_date, mm_net,
                   PERCENT_RANK() OVER (PARTITION BY commodity ORDER BY mm_net) as pct_rank
            FROM silver.cftc_position_history
            WHERE commodity IN ('corn', 'soybeans', 'wheat', 'soybean_oil', 'soybean_meal')
            ORDER BY report_date DESC
            LIMIT 20
        """)

        for row in cur.fetchall():
            if row['pct_rank'] and (row['pct_rank'] > 0.90 or row['pct_rank'] < 0.10):
                anomalies.append({
                    'type': 'positioning_extreme',
                    'commodity': row['commodity'],
                    'mm_net': float(row['mm_net']),
                    'percentile': float(row['pct_rank']) * 100,
                })
                logger.warning(
                    f"POSITIONING: {row['commodity']} MM net at "
                    f"{row['pct_rank']*100:.0f}th percentile ({row['mm_net']:,.0f} contracts)"
                )

    conn.close()
    return {'anomalies': anomalies, 'count': len(anomalies)}


# ═══════════════════════════════════════════════════════════════════
# STEP 4: ASSESS — Generate daily summary
# ═══════════════════════════════════════════════════════════════════

def generate_summary(today: date, verify_results: Dict, compute_results: Dict,
                     anomaly_results: Dict) -> str:
    """Generate a text summary for the morning brief."""

    lines = []
    lines.append(f"{'='*60}")
    lines.append(f"  DOC — Daily Operations Cycle")
    lines.append(f"  {today.strftime('%A, %B %d, %Y')}")
    lines.append(f"{'='*60}")

    # Collector status
    ok_count = len(verify_results.get('ok', []))
    missing_count = len(verify_results.get('missing', []))
    failed_count = len(verify_results.get('failed', []))
    total = ok_count + missing_count + failed_count

    if missing_count == 0 and failed_count == 0:
        lines.append(f"\n  DATA COLLECTION: ALL CLEAR ({ok_count}/{total})")
    else:
        lines.append(f"\n  DATA COLLECTION: ISSUES ({ok_count}/{total} OK)")
        for msg in verify_results.get('missing', []):
            lines.append(f"    MISSING: {msg}")
        for msg in verify_results.get('failed', []):
            lines.append(f"    FAILED:  {msg}")

    # Crush margins
    crush = compute_results.get('crush', {})
    if crush and 'soybeans' in crush:
        soy = crush['soybeans']
        lines.append(f"\n  SOYBEAN CRUSH: ${soy['margin']:.2f}/bu ({soy['margin_pct']:+.1f}%)")
        lines.append(f"    Oil: {soy['oil_price']:.2f}c/lb  Meal: ${soy['meal_price']:.2f}/ton  Beans: ${soy['seed_price']:.2f}/bu")

    # Feedstock allocation
    alloc = compute_results.get('allocation', {})
    if alloc.get('status') == 'complete':
        lines.append(f"\n  FEEDSTOCK ALLOCATION: Updated")
    elif alloc.get('status') == 'skipped':
        lines.append(f"\n  FEEDSTOCK ALLOCATION: Skipped ({alloc.get('reason', '')})")

    # Anomalies
    anomalies = anomaly_results.get('anomalies', [])
    if anomalies:
        lines.append(f"\n  ANOMALIES: {len(anomalies)} flagged")
        for a in anomalies[:5]:
            if a['type'] == 'price_move':
                lines.append(
                    f"    {a['symbol']}: {a['change']:+.2f} ({a['pct_change']:+.1f}%, "
                    f"z={a['z_score']:.1f})"
                )
            elif a['type'] == 'positioning_extreme':
                lines.append(
                    f"    {a['commodity']}: MM net {a['mm_net']:,.0f} "
                    f"({a['percentile']:.0f}th percentile)"
                )
            elif a['type'] == 'data_gap':
                lines.append(f"    {a['collector']}: {a['detail']}")
    else:
        lines.append(f"\n  ANOMALIES: None detected")

    lines.append(f"\n{'='*60}")
    lines.append(f"  DOC complete at {datetime.now().strftime('%H:%M:%S')}")
    lines.append(f"{'='*60}")

    return '\n'.join(lines)


# ═══════════════════════════════════════════════════════════════════
# STEP 5: ALERT — Save to event log for LLM briefing
# ═══════════════════════════════════════════════════════════════════

def save_to_event_log(today: date, summary: str, anomaly_count: int):
    """Save DOC results to core.event_log so the LLM briefing picks it up."""
    conn = get_conn()
    cur = conn.cursor()

    priority = 1 if anomaly_count > 0 else 3

    try:
        cur.execute("""
            SELECT id FROM core.event_log
            WHERE source = 'DOC' AND event_time::date = %s
            LIMIT 1
        """, (today,))

        if cur.fetchone():
            # Update existing
            cur.execute("""
                UPDATE core.event_log
                SET summary = %s, details = %s, priority = %s, event_time = NOW()
                WHERE source = 'DOC' AND event_time::date = %s
            """, (f"DOC: {anomaly_count} anomalies",
                  json.dumps({'summary': summary, 'anomaly_count': anomaly_count}),
                  priority, today))
        else:
            cur.execute("""
                INSERT INTO core.event_log (event_type, source, summary, details, priority)
                VALUES ('system_alert', 'DOC', %s, %s, %s)
            """, (f"DOC: {anomaly_count} anomalies",
                  json.dumps({'summary': summary, 'anomaly_count': anomaly_count}),
                  priority))

        conn.commit()
    except Exception as e:
        logger.error(f"Failed to save to event log: {e}")
        conn.rollback()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def run_full_cycle(today: date = None):
    """Run the complete Daily Operations Cycle."""
    today = today or date.today()
    logger.info(f"Starting DOC for {today}")

    # Step 1: Verify
    logger.info("Step 1: VERIFY — Checking collectors...")
    verify_results = verify_collectors(today)

    # Step 2: Compute
    logger.info("Step 2: COMPUTE — Running models...")
    compute_results = {
        'crush': compute_crush_margins(today),
        'allocation': compute_feedstock_allocation(today),
    }

    # Step 3: Detect
    logger.info("Step 3: DETECT — Scanning for anomalies...")
    anomaly_results = detect_anomalies(today)

    # Step 4: Assess
    logger.info("Step 4: ASSESS — Generating summary...")
    summary = generate_summary(today, verify_results, compute_results, anomaly_results)

    # Step 5: Alert
    logger.info("Step 5: ALERT — Saving to event log...")
    save_to_event_log(today, summary, anomaly_results['count'])

    # Print summary
    print(summary)

    return {
        'verify': verify_results,
        'compute': compute_results,
        'anomalies': anomaly_results,
        'summary': summary,
    }


def main():
    load_dotenv(Path(__file__).resolve().parents[3] / '.env')

    parser = argparse.ArgumentParser(description="DOC — Daily Operations Cycle")
    parser.add_argument("--verify", action="store_true", help="Just verify collectors")
    parser.add_argument("--compute", action="store_true", help="Just run models")
    parser.add_argument("--detect", action="store_true", help="Just anomaly detection")
    parser.add_argument("--summary", action="store_true", help="Print today's summary")
    parser.add_argument("--date", help="Run for specific date (YYYY-MM-DD)")
    args = parser.parse_args()

    today = date.fromisoformat(args.date) if args.date else date.today()

    if args.verify:
        results = verify_collectors(today)
        print(f"OK: {len(results['ok'])}")
        for msg in results['ok']:
            print(f"  {msg}")
        if results['missing']:
            print(f"\nMISSING: {len(results['missing'])}")
            for msg in results['missing']:
                print(f"  {msg}")
        if results['failed']:
            print(f"\nFAILED: {len(results['failed'])}")
            for msg in results['failed']:
                print(f"  {msg}")

    elif args.compute:
        crush = compute_crush_margins(today)
        alloc = compute_feedstock_allocation(today)
        print(f"Crush: {crush}")
        print(f"Allocation: {alloc}")

    elif args.detect:
        results = detect_anomalies(today)
        print(f"Anomalies: {results['count']}")
        for a in results['anomalies']:
            print(f"  {a}")

    elif args.summary:
        # Run everything and print summary
        run_full_cycle(today)

    else:
        run_full_cycle(today)


if __name__ == "__main__":
    main()
