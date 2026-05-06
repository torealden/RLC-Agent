"""
Market Field calibration — grid-search over (ALPHA, BETA, GAMMA, EPS).

Two calibration modes supported:

  --mode dispersion   (CROSS-FACILITY)
    Scores how well predicted sentiment matches expected per-facility
    response intensities. Requires a curated events JSON with
    expected_peak_facilities populated. Use when you have a few episodes
    you remember well and can specify which facilities should respond
    strongest with what magnitude.

  --mode direction    (TEMPORAL, default)
    Scores how well the network-wide average sentiment moves in the
    correct DIRECTION on each event date. Doesn't need facility
    specificity — just date + topic + expected polarity. Reads
    automatically from silver.market_event_consensus (chart-extracted
    historical events at agreement_score >= 0.67). This is the right
    mode for the bulk historical chart corpus.

Output: top-N parameter combos ranked by fitness (lower error = better).

Event JSON template:
  [
    {
      "event_date": "2026-05-05",
      "topic": "weather",
      "expected_origin_facilities": ["ia.cargill_iowa_falls"],
      "expected_polarity": -1.0,
      "expected_peak_facilities": [
          {"facility_id": "ia.cargill_fort_dodge", "peak_day_offset": 1, "magnitude": -0.4},
          {"facility_id": "ia.agp_eagle_grove",    "peak_day_offset": 1, "magnitude": -0.3}
      ],
      "notes": "Frost story originating in Iowa Falls, propagated to nearby plants in 1 day"
    },
    ...
  ]

Status: scaffolding ready. Needs user-supplied events to do anything useful.
The grid is intentionally coarse for v1 (~60 combinations); refine once events
are curated.
"""
from __future__ import annotations

import argparse
import itertools
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger('calibrate_market_field')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


# Default grid — coarse for v1. Refine once we have real events to fit against.
ALPHA_GRID = [0.75, 0.85, 0.92]
BETA_GRID  = [0.20, 0.30, 0.40]
GAMMA_GRID = [0.05, 0.10, 0.20]
EPS_GRID   = [0.01, 0.02]


def load_events(path: Path) -> list[dict]:
    if not path.exists():
        sys.exit(f'Events file not found: {path}\n'
                 f'See module docstring for the expected schema.')
    with path.open() as f:
        events = json.load(f)
    logger.info(f'Loaded {len(events)} calibration events.')
    return events


def score_combo(alpha: float, beta: float, gamma: float, eps: float,
                events: list[dict]) -> float:
    """Run the update loop with given coefficients across the relevant date
    windows, compute fit error against expected dispersion patterns.

    Returns total error — lower is better.
    """
    # Lazy import so this script can show its template + docstring even when DB unreachable
    import importlib
    mod = importlib.import_module('scripts.update_facility_sentiment')
    # Override coefficients in the imported module's namespace
    mod.ALPHA, mod.BETA, mod.GAMMA, mod.EPS = alpha, beta, gamma, eps

    total_err = 0.0
    for ev in events:
        ev_date = datetime.strptime(ev['event_date'], '%Y-%m-%d').date()
        # Re-run update for the event window (event date and surrounding days)
        for day_offset in range(-2, 4):
            d = ev_date + timedelta(days=day_offset)
            mod.update_for_date(d, recompute=True, rng_seed=42)

        # Score: read computed sentiments and compare to expected_peak_facilities
        from src.services.database.db_config import get_connection
        import psycopg2.extras
        with get_connection() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            for peak in ev.get('expected_peak_facilities', []):
                target_date = ev_date + timedelta(days=peak['peak_day_offset'])
                cur.execute(
                    """SELECT topic_sentiments
                       FROM gold.facility_sentiment_daily
                       WHERE facility_id=%s AND as_of_date=%s
                         AND market_id='us_oilseed_crush'
                         AND classifier_version='mf-v1'""",
                    (peak['facility_id'], target_date),
                )
                row = cur.fetchone()
                if not row:
                    total_err += 1.0   # missing data is worst-case
                    continue
                actual = float(row['topic_sentiments'].get(ev['topic'], 0))
                expected = peak['magnitude']
                err = abs(actual - expected)
                total_err += err

    return total_err


def load_chart_events(min_agreement: float = 0.67,
                      date_window_start: Optional[str] = None,
                      date_window_end: Optional[str] = None) -> list[dict]:
    """Pull high-agreement events from silver.market_event_consensus, filtered
    to the topic_keys our Market Field knows about."""
    from src.services.database.db_config import get_connection
    import psycopg2.extras
    valid_topics = {'weather', 'soybean_supply', 'veg_oil_demand',
                    'meal_livestock_demand', 'policy_federal',
                    'policy_state_local', 'policy_industry', 'competitor_activity'}
    sql = """
        SELECT consensus_date, topic_key, consensus_polarity, consensus_intensity,
               agreement_score, polarity_stdev, consensus_text, chart_contract
        FROM silver.market_event_consensus
        WHERE agreement_score >= %s
          AND consensus_date IS NOT NULL
          AND consensus_polarity IS NOT NULL
    """
    params = [min_agreement]
    if date_window_start:
        sql += ' AND consensus_date >= %s'
        params.append(date_window_start)
    if date_window_end:
        sql += ' AND consensus_date <= %s'
        params.append(date_window_end)
    sql += ' ORDER BY consensus_date'
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
    return [r for r in rows if r['topic_key'] in valid_topics]


def score_direction(alpha: float, beta: float, gamma: float, eps: float,
                    events: list[dict]) -> tuple[float, dict]:
    """Run the update loop with given coefficients. For each event, compare
    the observed network-wide sentiment change on the event date against the
    expected polarity. Lower error = better fit.

    Returns (total_error, diagnostics).
    """
    import importlib
    mod = importlib.import_module('scripts.update_facility_sentiment')
    mod.ALPHA, mod.BETA, mod.GAMMA, mod.EPS = alpha, beta, gamma, eps

    # Group events by date so we re-compute the day once
    from collections import defaultdict
    events_by_date = defaultdict(list)
    for ev in events:
        events_by_date[ev['consensus_date']].append(ev)

    # For each event date, recompute (date-1, date, date+1) sentiment
    from datetime import timedelta
    dates_to_compute = set()
    for d in events_by_date:
        for off in (-1, 0, 1):
            dates_to_compute.add(d + timedelta(days=off))
    for d in sorted(dates_to_compute):
        try:
            mod.update_for_date(d, recompute=True, rng_seed=42)
        except Exception:
            pass

    from src.services.database.db_config import get_connection
    import psycopg2.extras
    total_err = 0.0
    n_scored = 0
    n_direction_correct = 0
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        for d, evs in events_by_date.items():
            for ev in evs:
                topic = ev['topic_key']
                # Average sentiment across all facilities on (d-1) and (d+1)
                cur.execute("""
                    SELECT as_of_date, AVG((topic_sentiments->>%s)::numeric) AS avg_s
                    FROM gold.facility_sentiment_daily
                    WHERE market_id='us_oilseed_crush'
                      AND classifier_version='mf-v1'
                      AND as_of_date IN (%s, %s)
                    GROUP BY as_of_date
                """, (topic, d - timedelta(days=1), d + timedelta(days=1)))
                rows = {r['as_of_date']: float(r['avg_s'] or 0) for r in cur.fetchall()}
                pre = rows.get(d - timedelta(days=1), 0.0)
                post = rows.get(d + timedelta(days=1), 0.0)
                if not rows:
                    continue
                observed_change = post - pre
                expected_polarity = float(ev['consensus_polarity'])
                expected_intensity = float(ev['consensus_intensity'] or 0.5)

                # Direction error: 0 if same sign, +1 if opposite
                same_sign = (observed_change >= 0) == (expected_polarity >= 0)
                if same_sign:
                    n_direction_correct += 1
                err = 0.0 if same_sign else 1.0
                # Add magnitude alignment penalty (small)
                expected_mag = abs(expected_polarity) * expected_intensity * 0.3
                mag_err = abs(abs(observed_change) - expected_mag)
                err += mag_err * 0.5
                total_err += err
                n_scored += 1

    diagnostics = {
        'n_scored': n_scored,
        'n_direction_correct': n_direction_correct,
        'direction_accuracy': (n_direction_correct / n_scored) if n_scored else 0,
    }
    return total_err, diagnostics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--mode', choices=['direction', 'dispersion'], default='direction',
                    help='direction (default) uses chart corpus; dispersion uses curated JSON')
    ap.add_argument('--events', type=Path,
                    default=Path('clients/market_field_calibration_events.json'),
                    help='Curated events JSON (dispersion mode only)')
    ap.add_argument('--min-agreement', type=float, default=0.67,
                    help='Min agreement_score to include event (direction mode)')
    ap.add_argument('--start', type=str,
                    help='Earliest event_date YYYY-MM-DD (direction mode)')
    ap.add_argument('--end', type=str,
                    help='Latest event_date YYYY-MM-DD (direction mode)')
    ap.add_argument('--top', type=int, default=5,
                    help='Show top N parameter combos')
    args = ap.parse_args()

    if args.mode == 'dispersion':
        events = load_events(args.events)
        if not events:
            sys.exit('No events to calibrate against.')
        scoring_fn = score_combo
    else:
        events = load_chart_events(args.min_agreement, args.start, args.end)
        if not events:
            sys.exit('No chart events match criteria. Check agreement threshold + date window. '
                     'Note: chart corpus is 1969-2016 but news+sentiment data only exists for '
                     'recent dates — direction mode only scores events that overlap.')
        logger.info(f'Loaded {len(events)} chart events with agreement >= {args.min_agreement}')
        scoring_fn = score_direction

    combos = list(itertools.product(ALPHA_GRID, BETA_GRID, GAMMA_GRID, EPS_GRID))
    logger.info(f'Grid: {len(combos)} (alpha, beta, gamma, eps) combinations')

    results = []
    for i, (a, b, g, e) in enumerate(combos):
        result = scoring_fn(a, b, g, e, events)
        if isinstance(result, tuple):
            err, diag = result
        else:
            err, diag = result, {}
        results.append(((a, b, g, e), err, diag))
        if i % 5 == 0:
            logger.info(f'  ... {i}/{len(combos)} done')

    results.sort(key=lambda r: r[1])
    print()
    print(f'Top {args.top} parameter sets (lower error = better fit):')
    if args.mode == 'direction':
        print(f'{"alpha":>6s} {"beta":>6s} {"gamma":>6s} {"eps":>6s} '
              f'{"error":>10s} {"dir-acc":>8s} {"n":>4s}')
        for (a, b, g, e), err, diag in results[:args.top]:
            acc = diag.get('direction_accuracy', 0) * 100
            n = diag.get('n_scored', 0)
            print(f'  {a:>6.2f} {b:>6.2f} {g:>6.2f} {e:>6.2f} '
                  f'{err:>10.4f} {acc:>7.1f}% {n:>4}')
    else:
        print(f'{"alpha":>6s} {"beta":>6s} {"gamma":>6s} {"eps":>6s} {"error":>10s}')
        for (a, b, g, e), err, _ in results[:args.top]:
            print(f'  {a:>6.2f} {b:>6.2f} {g:>6.2f} {e:>6.2f} {err:>10.4f}')


if __name__ == '__main__':
    main()
