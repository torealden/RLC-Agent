"""
Market Field calibration — grid-search over (ALPHA, BETA, GAMMA, EPS) to
fit observed sentiment dispersion against user-specified historical events.

Usage flow:
  1. User curates a list of historical events with EXPECTED dispersion patterns
     in clients/market_field_calibration_events.json (template below). For each
     event: date, story type, which facilities should respond strongest, expected
     direction, expected propagation speed (days to peak influence on neighbors).
  2. Run: python -m scripts.calibrate_market_field --events <path>
  3. Script grid-searches the four coefficients, runs the update loop with each
     combination on the days surrounding each event, scores the fit, returns
     the top-N parameter sets.

Fitness function: for each event, score = sum over (facility, day) of
   abs( predicted_sentiment_shift - expected_shift )
weighted by intensity_expected. Lower = better fit. Aggregate across events.

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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--events', type=Path,
                    default=Path('clients/market_field_calibration_events.json'),
                    help='JSON file with curated historical events')
    ap.add_argument('--top', type=int, default=5,
                    help='Show top N parameter combos')
    args = ap.parse_args()

    events = load_events(args.events)
    if not events:
        sys.exit('No events to calibrate against.')

    combos = list(itertools.product(ALPHA_GRID, BETA_GRID, GAMMA_GRID, EPS_GRID))
    logger.info(f'Grid: {len(combos)} (alpha, beta, gamma, eps) combinations')

    results = []
    for i, (a, b, g, e) in enumerate(combos):
        err = score_combo(a, b, g, e, events)
        results.append(((a, b, g, e), err))
        if i % 5 == 0:
            logger.info(f'  ... {i}/{len(combos)} done')

    results.sort(key=lambda r: r[1])
    print()
    print(f'Top {args.top} parameter sets (lower error = better fit):')
    print(f'{"alpha":>6s} {"beta":>6s} {"gamma":>6s} {"eps":>6s} {"error":>10s}')
    for (a, b, g, e), err in results[:args.top]:
        print(f'  {a:>6.2f} {b:>6.2f} {g:>6.2f} {e:>6.2f} {err:>10.4f}')


if __name__ == '__main__':
    main()
