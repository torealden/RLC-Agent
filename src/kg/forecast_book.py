"""
LLM Forecast Book — parallel forecasts to human spreadsheets.

For every monthly data series we track in spreadsheets, the LLM maintains
a separate forecast in core.forecasts. When realized data arrives, we
populate core.forecast_actual_pairs and score. Error attribution feeds
KG refinement (hook for task: "reconciliation drives learning").

This module is the orchestration layer — not the LLM itself. The LLM is the
caller; this module gives it structured access to:
  - Read KG context to inform a forecast
  - Invoke kg_callables to compute a number
  - Record a forecast with citation + confidence band
  - Look up the most recent actual to reconcile against

Design goals:
  1. Each forecast is reproducible — the inputs that produced it are logged
     in the notes field as JSON
  2. Citations are first-class — every recorded forecast points back to the
     KG contexts / callables / data series it relied on
  3. Reconciliation is automatic when actuals land
  4. Human forecasts and LLM forecasts live in the SAME table distinguished
     by the `source` field, so accuracy comparison is trivial SQL
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / '.env')


SOURCE_LLM = 'llm_forecast_book'
SOURCE_HUMAN = 'analyst_spreadsheet'
SOURCE_USDA = 'usda_wasde'


def _connect():
    return psycopg2.connect(
        host=os.environ['RLC_PG_HOST'],
        port=os.environ.get('RLC_PG_PORT', 5432),
        database=os.environ.get('RLC_PG_DATABASE', 'rlc_commodities'),
        user=os.environ['RLC_PG_USER'],
        password=os.environ['RLC_PG_PASSWORD'],
        sslmode='require',
    )


# =============================================================================
# Recording a forecast
# =============================================================================

def record_forecast(
    commodity: str,
    forecast_type: str,
    target_date: date,
    value: float,
    unit: str,
    *,
    country: str = 'US',
    confidence_low: Optional[float] = None,
    confidence_high: Optional[float] = None,
    marketing_year: Optional[str] = None,
    source: str = SOURCE_LLM,
    analyst: str = 'llm_forecast_book',
    reasoning: Optional[str] = None,
    kg_citations: Optional[list] = None,
    callable_invocations: Optional[list] = None,
    inputs: Optional[dict] = None,
    forecast_date: Optional[date] = None,
    forecast_id: Optional[str] = None,
) -> str:
    """
    Write a single forecast to core.forecasts. Returns the forecast_id.

    All provenance (inputs, reasoning, citations, callable invocations) is
    stored as structured JSON in the `notes` field so we can replay any
    forecast later.
    """
    forecast_id = forecast_id or f"{source}:{commodity}:{forecast_type}:{target_date.isoformat()}:{uuid.uuid4().hex[:8]}"
    forecast_date = forecast_date or date.today()

    notes = json.dumps({
        'reasoning': reasoning,
        'kg_citations': kg_citations or [],
        'callable_invocations': callable_invocations or [],
        'inputs': inputs or {},
        'recorded_at': datetime.utcnow().isoformat() + 'Z',
    }, default=str)

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO core.forecasts
                (forecast_id, forecast_date, target_date, commodity, country,
                 forecast_type, value, unit, confidence_low, confidence_high,
                 marketing_year, notes, source, analyst, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (forecast_id, forecast_date, target_date, commodity, country,
              forecast_type, value, unit, confidence_low, confidence_high,
              marketing_year, notes, source, analyst))
        conn.commit()
    finally:
        conn.close()
    return forecast_id


# =============================================================================
# Lookups
# =============================================================================

def get_latest_forecast(commodity: str, forecast_type: str,
                        target_date: date, source: str = SOURCE_LLM) -> Optional[dict]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT forecast_id, forecast_date, value, unit, confidence_low, confidence_high,
                   marketing_year, source, analyst, notes
            FROM core.forecasts
            WHERE commodity=%s AND forecast_type=%s AND target_date=%s AND source=%s
            ORDER BY forecast_date DESC LIMIT 1
        """, (commodity, forecast_type, target_date, source))
        row = cur.fetchone()
        if not row:
            return None
        return {
            'forecast_id': row[0], 'forecast_date': row[1], 'value': row[2],
            'unit': row[3], 'confidence_low': row[4], 'confidence_high': row[5],
            'marketing_year': row[6], 'source': row[7], 'analyst': row[8],
            'notes': json.loads(row[9]) if row[9] else None,
        }
    finally:
        conn.close()


def get_forecasts_for_target(commodity: str, forecast_type: str,
                             target_date: date) -> List[dict]:
    """Return all forecasts for a specific target date — one per source."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (source) source, value, confidence_low, confidence_high,
                   forecast_date, analyst, forecast_id
            FROM core.forecasts
            WHERE commodity=%s AND forecast_type=%s AND target_date=%s
            ORDER BY source, forecast_date DESC
        """, (commodity, forecast_type, target_date))
        return [{
            'source': r[0], 'value': r[1], 'ci_low': r[2], 'ci_high': r[3],
            'as_of': r[4], 'analyst': r[5], 'forecast_id': r[6],
        } for r in cur.fetchall()]
    finally:
        conn.close()


# =============================================================================
# Reconciliation
# =============================================================================

def record_actual(commodity: str, forecast_type: str, target_date: date,
                  value: float, unit: str,
                  *, country: str = 'US', source: str = 'nass_realized',
                  marketing_year: Optional[str] = None,
                  revision_number: int = 0) -> str:
    """
    Record the realized/actual value in core.actuals.

    Note: actuals live in a dedicated table (not core.forecasts) because
    core.forecast_actual_pairs.actual_id has a FK to core.actuals.actual_id.
    value_type is the forecast_type (e.g., 'monthly_crush_mbu').
    """
    actual_id = f"{source}:{commodity}:{forecast_type}:{target_date.isoformat()}:rev{revision_number}"
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO core.actuals
                (actual_id, report_date, target_date, commodity, country,
                 value_type, value, unit, marketing_year, source, revision_number, notes, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (actual_id) DO UPDATE SET
                value = EXCLUDED.value,
                report_date = EXCLUDED.report_date,
                revision_number = EXCLUDED.revision_number
        """, (actual_id, date.today(), target_date, commodity, country,
              forecast_type, value, unit, marketing_year, source,
              revision_number, f'Recorded via forecast_book.record_actual'))
        conn.commit()
    finally:
        conn.close()
    return actual_id


def reconcile(commodity: str, forecast_type: str, target_date: date,
              forecast_source: str = SOURCE_LLM) -> Optional[dict]:
    """
    Find the latest forecast (core.forecasts) and actual (core.actuals) for
    the series + target, then write a pair to core.forecast_actual_pairs.
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT forecast_id, value, forecast_date
            FROM core.forecasts
            WHERE commodity=%s AND forecast_type=%s AND target_date=%s AND source=%s
            ORDER BY forecast_date DESC LIMIT 1
        """, (commodity, forecast_type, target_date, forecast_source))
        f = cur.fetchone()
        if not f:
            return None
        f_id, f_val, f_date = f

        cur.execute("""
            SELECT actual_id, value, report_date
            FROM core.actuals
            WHERE commodity=%s AND value_type=%s AND target_date=%s
            ORDER BY revision_number DESC, report_date DESC LIMIT 1
        """, (commodity, forecast_type, target_date))
        a = cur.fetchone()
        if not a:
            return None
        a_id, a_val, a_date = a

        error = f_val - a_val
        pct_error = (error / a_val * 100) if a_val else None
        abs_error = abs(error)
        abs_pct_error = abs(pct_error) if pct_error is not None else None
        direction_correct = 1  # For point forecasts we'd need a prior anchor to score direction

        days_ahead = (a_date - f_date).days if f_date and a_date else None

        cur.execute("""
            INSERT INTO core.forecast_actual_pairs
                (forecast_id, actual_id, error, percentage_error,
                 absolute_error, absolute_percentage_error,
                 direction_correct, days_ahead, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (f_id, a_id, error, pct_error, abs_error, abs_pct_error,
              direction_correct, days_ahead))
        conn.commit()
        return {
            'forecast_id': f_id, 'actual_id': a_id,
            'forecast_value': f_val, 'actual_value': a_val,
            'error': error, 'pct_error': pct_error, 'days_ahead': days_ahead,
        }
    finally:
        conn.close()


# =============================================================================
# Accuracy reporting
# =============================================================================

def accuracy_summary(commodity: str, forecast_type: str,
                     source: str = SOURCE_LLM, since_days: int = 365) -> dict:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*), AVG(p.percentage_error), AVG(p.absolute_percentage_error),
                   AVG(p.absolute_error)
            FROM core.forecast_actual_pairs p
            JOIN core.forecasts f ON f.forecast_id = p.forecast_id
            WHERE f.commodity=%s AND f.forecast_type=%s AND f.source=%s
              AND p.created_at > NOW() - INTERVAL '%s days'
        """, (commodity, forecast_type, source, since_days))
        n, mpe, mape, mae = cur.fetchone()
        return {
            'n_pairs': n or 0,
            'mean_pct_error': float(mpe) if mpe is not None else None,
            'mean_abs_pct_error': float(mape) if mape is not None else None,
            'mean_abs_error': float(mae) if mae is not None else None,
            'window_days': since_days,
            'source': source,
        }
    finally:
        conn.close()


# =============================================================================
# Convenience: build the input bundle an LLM needs to reason
# =============================================================================

def build_forecast_context(commodity: str, forecast_type: str,
                           target_date: date) -> dict:
    """
    Assemble the reference pack an LLM should consult to produce a forecast:
      - relevant KG nodes + their contexts
      - any kg_callables attached to those nodes
      - last 6 recorded actuals for this series (for baseline)
      - any other-source forecast for the same target_date (so LLM knows
        where human / USDA are)

    Returns a dict ready to drop into an LLM prompt.
    """
    conn = _connect()
    try:
        cur = conn.cursor()

        # 1. Relevant KG contexts (commodity + report_node if known)
        cur.execute("""
            SELECT n.node_key, n.node_type, n.label, c.context_type, c.context_key, c.context_value
            FROM core.kg_node n
            JOIN core.kg_context c ON c.node_id = n.id
            WHERE n.node_key = %s OR n.node_key LIKE %s OR n.node_key LIKE %s
            ORDER BY n.node_key, c.context_type, c.context_key
        """, (commodity, f'%{commodity}%', f'%{forecast_type}%'))
        contexts = [{'node_key': r[0], 'node_type': r[1], 'label': r[2],
                     'context_type': r[3], 'context_key': r[4],
                     'context_value': r[5]} for r in cur.fetchall()]

        # 2. Callables that could help
        cur.execute("""
            SELECT c.callable_key, c.label, c.callable_type, n.node_key
            FROM core.kg_callable c JOIN core.kg_node n ON n.id=c.node_id
            WHERE c.status = 'draft' OR c.status = 'active'
              AND (n.node_key = %s OR n.node_key LIKE %s)
        """, (commodity, f'%{commodity}%'))
        callables = [{'callable_key': r[0], 'label': r[1],
                      'callable_type': r[2], 'attached_to_node': r[3]} for r in cur.fetchall()]

        # 3. Recent actuals (last 6 months) from core.actuals
        cur.execute("""
            SELECT report_date, target_date, value, unit, source
            FROM core.actuals
            WHERE commodity=%s AND value_type=%s
            ORDER BY target_date DESC LIMIT 6
        """, (commodity, forecast_type))
        recent_actuals = [{'target_date': r[1], 'value': r[2], 'unit': r[3],
                           'source': r[4]} for r in cur.fetchall()]

        # 4. Competing forecasts for this target
        cur.execute("""
            SELECT DISTINCT ON (source) source, value, confidence_low, confidence_high, analyst
            FROM core.forecasts
            WHERE commodity=%s AND forecast_type=%s AND target_date=%s
              AND source NOT LIKE '%%_actual'
            ORDER BY source, forecast_date DESC
        """, (commodity, forecast_type, target_date))
        competing = [{'source': r[0], 'value': r[1], 'ci_low': r[2],
                      'ci_high': r[3], 'analyst': r[4]} for r in cur.fetchall()]

        return {
            'commodity': commodity,
            'forecast_type': forecast_type,
            'target_date': str(target_date),
            'kg_contexts': contexts,
            'callables_available': callables,
            'recent_actuals': recent_actuals,
            'competing_forecasts': competing,
        }
    finally:
        conn.close()


# =============================================================================
# CLI smoke test — pilot with a NOPA monthly soy crush forecast
# =============================================================================

if __name__ == '__main__':
    from pprint import pprint

    print("=" * 70)
    print("FORECAST BOOK SMOKE TEST — US soybean monthly crush")
    print("=" * 70)

    # 1. Gather the context an LLM would use
    ctx = build_forecast_context(
        commodity='soybeans',
        forecast_type='monthly_crush_mbu',
        target_date=date(2026, 5, 1),
    )
    print(f"\nContext bundle:")
    print(f"  KG contexts available: {len(ctx['kg_contexts'])}")
    print(f"  Callables available:   {len(ctx['callables_available'])}")
    print(f"  Recent actuals:        {len(ctx['recent_actuals'])}")
    print(f"  Competing forecasts:   {len(ctx['competing_forecasts'])}")
    if ctx['kg_contexts']:
        print("\n  Sample KG context keys:")
        for c in ctx['kg_contexts'][:5]:
            print(f"    - {c['node_key']} / {c['context_type']} / {c['context_key']}")

    # 2. Record an LLM forecast (simulated — in production the LLM produces the value)
    fid = record_forecast(
        commodity='soybeans',
        forecast_type='monthly_crush_mbu',
        target_date=date(2026, 5, 1),
        value=192.5,
        unit='million_bushels',
        confidence_low=188.0, confidence_high=196.5,
        marketing_year='2025/26',
        reasoning=(
            "NOPA member share ~95%. WASDE 25/26 annual crush 2,420 mbu. "
            "May typical seasonal share 8.1% = 196 mbu for membership; NOPA-only "
            "adjustment ~-3.5 mbu for margin pressure from high bean basis. "
            "Point estimate 192.5 with ±4 mbu band per historical monthly residual."
        ),
        kg_citations=[
            {'node_key': 'nopa.monthly', 'context_key': 'pace_vs_wasde_corn'},
            {'node_key': 'soybeans', 'context_key': 'pace_tracking_soy_crush'},
        ],
        inputs={
            'wasde_annual_mbu': 2420, 'seasonal_may_share_pct': 8.1,
            'nopa_coverage_pct': 95, 'basis_adjustment_mbu': -3.5,
        },
    )
    print(f"\nRecorded LLM forecast: {fid}")

    # 3. Show the latest forecast back
    latest = get_latest_forecast('soybeans', 'monthly_crush_mbu', date(2026, 5, 1))
    print(f"\nLatest forecast for soybeans monthly_crush_mbu May-2026:")
    pprint({k: v for k, v in latest.items() if k != 'notes'})
    print(f"  (notes contains reasoning + citations + inputs)")

    # 4. Show competing forecasts (will be just this one until human/WASDE added)
    competing = get_forecasts_for_target('soybeans', 'monthly_crush_mbu', date(2026, 5, 1))
    print(f"\nAll forecasts for May-2026 target ({len(competing)} sources):")
    for c in competing:
        print(f"  {c['source']}: {c['value']} [{c['ci_low']}, {c['ci_high']}] by {c['analyst']}")
