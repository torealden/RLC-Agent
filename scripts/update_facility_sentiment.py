"""
Market Field Layer 1, step 6: Daily sentiment update loop.

For each (facility, topic, day), computes the next sentiment value via:

  s_{i,k}(t+1) = ALPHA  * s_{i,k}(t)               // decay/inertia
              + BETA   * news_{i,k}(t)             // local exogenous
              + GAMMA  * sum_j w_ij * s_{j,k}(t)   // network influence (row-normalised)
              + EPS    * jump_{i,k}(t)             // weak-tie randomness

  Then clip to [-1, +1].

Output: one row per (market_id, facility_id, as_of_date) in
gold.facility_sentiment_daily, including a contribution_breakdown
JSONB so we can diagnose which term drove each sentiment shift.

Topic weighting:
  veg_oil_demand        scaled by current oil_share
  meal_livestock_demand scaled by 1 - oil_share
  (other topics: flat 1.0)

Usage:
    # Compute for a single date (default: yesterday)
    python -m scripts.update_facility_sentiment

    # Backfill from a start date forward (sequential — each day depends on
    # the prior day's sentiment, so order matters)
    python -m scripts.update_facility_sentiment --start 2026-04-25 --end 2026-05-06

    # Recompute (overwrites existing rows for the date)
    python -m scripts.update_facility_sentiment --as-of 2026-05-05 --recompute
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

import psycopg2.extras
from src.services.database.db_config import get_connection


logger = logging.getLogger('update_facility_sentiment')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


MARKET_ID = 'us_oilseed_crush'
CLASSIFIER_VERSION = 'mf-v1'

# Update coefficients — calibration v1, will tune from observed dispersion
ALPHA = 0.85   # decay / inertia
BETA  = 0.30   # exogenous (news) forcing
GAMMA = 0.10   # network influence
EPS   = 0.02   # weak-tie random jump probability magnitude

# Reversal logic (per spec — Iran-war-style sentiment flip on contradicting events):
# When today's news has opposite sign from yesterday's sentiment AND meaningful
# intensity, the decay term is suppressed and the news term gets the freed
# weight. This lets sentiment snap toward the new direction in one step
# rather than smoothing through many days of decay.
REVERSAL_THRESHOLD     = 0.4  # |news_force| must exceed this for reversal to fire
REVERSAL_BETA_BOOST    = 1.0  # absorb ALPHA's weight into BETA when reversing
                              # (so total mass on the news term = BETA + ALPHA*REVERSAL_BETA_BOOST)

# Fallback oil_share if silver.oilseed_crush_margin has no row for this date
DEFAULT_OIL_SHARE = 0.45


@dataclass
class Topic:
    key: str
    weighting_function: str  # 'flat', 'oil_share', 'meal_share'


def get_topics() -> list[Topic]:
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT topic_key, weighting_function FROM reference.market_topic_taxonomy
            WHERE market_id=%s AND is_active=TRUE ORDER BY sort_order
        """, (MARKET_ID,))
        return [Topic(r['topic_key'], r['weighting_function']) for r in cur.fetchall()]


def get_canonical_facilities() -> list[str]:
    """Geocoded canonical facilities — these are the only nodes in the graph."""
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT facility_id FROM reference.oilseed_crush_facilities
            WHERE state='IA' AND is_canonical=TRUE
              AND lat IS NOT NULL AND lat <> 0
            ORDER BY facility_id
        """)
        return [r['facility_id'] for r in cur.fetchall()]


def get_oil_share(as_of: date) -> float:
    """Latest available oil_share <= as_of, from silver.oilseed_crush_margin."""
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT oil_revenue_per_unit / NULLIF(oil_revenue_per_unit + meal_revenue_per_unit, 0) AS oil_share
            FROM silver.oilseed_crush_margin
            WHERE oilseed_code='soybeans' AND period <= %s
              AND oil_revenue_per_unit IS NOT NULL AND meal_revenue_per_unit IS NOT NULL
            ORDER BY period DESC LIMIT 1
        """, (as_of,))
        row = cur.fetchone()
        return float(row['oil_share']) if row and row['oil_share'] is not None else DEFAULT_OIL_SHARE


def get_edges() -> dict[str, dict[str, float]]:
    """Build aggregate (source -> target -> aggregated_weight) row-normalised.

    Per source facility, sum all edge_types for each target, then divide by
    the row total so neighbors are properly weighted in the network sum.
    """
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT source_facility_id, target_facility_id, SUM(weight) AS w
            FROM reference.facility_edge_weights
            WHERE market_id=%s AND is_active=TRUE
            GROUP BY source_facility_id, target_facility_id
        """, (MARKET_ID,))
        rows = cur.fetchall()

    raw = defaultdict(dict)
    for r in rows:
        raw[r['source_facility_id']][r['target_facility_id']] = float(r['w'])

    # Row-normalise: neighbors' weights sum to 1.0 per source
    normed = {}
    for src, neighbors in raw.items():
        total = sum(neighbors.values())
        normed[src] = {tgt: w / total for tgt, w in neighbors.items()} if total > 0 else {}
    return normed


def get_yesterday_sentiment(prev_date: date, facilities: list[str],
                            topics: list[Topic]) -> dict[tuple[str, str], float]:
    """Pull s_{i,k}(t-1) for every (facility, topic). Default 0 for missing."""
    sentiments = {(f, t.key): 0.0 for f in facilities for t in topics}
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT facility_id, topic_sentiments
            FROM gold.facility_sentiment_daily
            WHERE market_id=%s AND as_of_date=%s AND classifier_version=%s
        """, (MARKET_ID, prev_date, CLASSIFIER_VERSION))
        for r in cur.fetchall():
            for topic_key, v in (r['topic_sentiments'] or {}).items():
                key = (r['facility_id'], topic_key)
                if key in sentiments:
                    sentiments[key] = float(v)
    return sentiments


def get_news_for_date(as_of: date) -> list[dict]:
    """Articles classified for as_of (using published_at; fall back to fetched_at)."""
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT n.id, n.source_name, n.title,
                   COALESCE(n.published_at, n.fetched_at) AS effective_date,
                   c.topic_scores, c.locality, c.facility_relevance_keys,
                   c.confidence_score, ns.source_weight
            FROM bronze.news_article n
            JOIN silver.news_classified c
              ON c.news_article_id = n.id AND c.market_id=%s
                 AND c.classifier_version=%s
            LEFT JOIN reference.news_source ns ON ns.source_name = n.source_name
            WHERE DATE(COALESCE(n.published_at, n.fetched_at) AT TIME ZONE 'UTC') = %s
        """, (MARKET_ID, CLASSIFIER_VERSION, as_of))
        return [dict(r) for r in cur.fetchall()]


def compute_news_force(news: list[dict], facilities: list[str],
                       topics: list[Topic], oil_share: float) -> dict[tuple[str, str], float]:
    """Aggregate news_{i,k}(t) per (facility, topic) for one day.

    Each article contributes to facilities based on its locality:
      facility   -> only listed facility_relevance_keys
      local      -> only listed facility_relevance_keys (or all if empty — fallback)
      regional   -> all canonical facilities (Iowa is one region for the pilot)
      national   -> all canonical facilities

    Topic contribution = polarity * intensity * source_weight * topic_weight.
    Topic weight is 1.0 except for veg_oil_demand (* oil_share) and
    meal_livestock_demand (* 1 - oil_share).
    """
    topic_weight = {
        t.key: (oil_share if t.weighting_function == 'oil_share'
                else (1 - oil_share) if t.weighting_function == 'meal_share'
                else 1.0)
        for t in topics
    }

    force = {(f, t.key): 0.0 for f in facilities for t in topics}

    for art in news:
        scores = art.get('topic_scores') or {}
        if not scores:
            continue
        sw = float(art.get('source_weight') or 1.0)
        relevant = (art.get('facility_relevance_keys') or []) if art['locality'] in ('facility', 'local') \
            else facilities
        if not relevant:
            relevant = facilities  # fallback when locality says facility/local but no keys provided

        for fac in relevant:
            for topic_key, scored in scores.items():
                if not isinstance(scored, dict):
                    continue
                pol = float(scored.get('polarity') or 0)
                inten = float(scored.get('intensity') or 0)
                tw = topic_weight.get(topic_key, 1.0)
                contribution = pol * inten * sw * tw
                if (fac, topic_key) in force:
                    force[(fac, topic_key)] += contribution

    return force


def compute_network_force(yesterday: dict[tuple[str, str], float],
                          edges: dict[str, dict[str, float]],
                          facilities: list[str],
                          topic_keys: list[str]) -> dict[tuple[str, str], float]:
    """sum_j w_ij * s_{j,k}(t-1) per (facility, topic). Edges are pre-row-normalised."""
    network = {(f, k): 0.0 for f in facilities for k in topic_keys}
    for src in facilities:
        neighbors = edges.get(src, {})
        for k in topic_keys:
            s = sum(w * yesterday.get((tgt, k), 0.0)
                    for tgt, w in neighbors.items())
            network[(src, k)] = s
    return network


def sample_jump(rng: random.Random, p: float = 0.05) -> float:
    """Weak-tie randomness. Most days returns 0; rare days returns sampled
    sentiment in [-1, +1] to model 'unrelated story breaks through'."""
    if rng.random() >= p:
        return 0.0
    return rng.uniform(-1, 1)


def update_for_date(as_of: date, recompute: bool = False,
                    rng_seed: Optional[int] = None) -> int:
    """Run the update for one date. Returns rows persisted."""
    facilities = get_canonical_facilities()
    topics = get_topics()
    topic_keys = [t.key for t in topics]
    edges = get_edges()
    oil_share = get_oil_share(as_of)
    yesterday = get_yesterday_sentiment(as_of - timedelta(days=1), facilities, topics)
    news = get_news_for_date(as_of)

    logger.info(f'as_of={as_of}  facilities={len(facilities)}  topics={len(topic_keys)}'
                f'  edges_src={len(edges)}  news_count={len(news)}'
                f'  oil_share={oil_share:.3f}')

    news_force = compute_news_force(news, facilities, topics, oil_share)
    network_force = compute_network_force(yesterday, edges, facilities, topic_keys)
    rng = random.Random(rng_seed if rng_seed is not None else int(as_of.toordinal()))

    rows_to_persist = []
    n_reversals = 0
    for fac in facilities:
        topic_sentiments = {}
        contribution_breakdown = {}
        for k in topic_keys:
            s_prev = yesterday[(fac, k)]
            n = news_force[(fac, k)]
            net = network_force[(fac, k)]
            jump = sample_jump(rng)

            # Reversal trigger: meaningful news with opposite sign to yesterday
            is_reversal = (
                abs(n) >= REVERSAL_THRESHOLD
                and abs(s_prev) > 0.05
                and (n > 0) != (s_prev > 0)
            )
            if is_reversal:
                # Suppress decay; redirect ALPHA's weight to BETA
                effective_alpha = 0.0
                effective_beta = BETA + ALPHA * REVERSAL_BETA_BOOST
                n_reversals += 1
            else:
                effective_alpha = ALPHA
                effective_beta = BETA

            decay_term     = effective_alpha * s_prev
            exogenous_term = effective_beta * n
            network_term   = GAMMA * net
            jump_term      = EPS * jump
            new_s = decay_term + exogenous_term + network_term + jump_term
            new_s = max(-1.0, min(1.0, new_s))
            topic_sentiments[k] = round(new_s, 5)
            contribution_breakdown[k] = {
                'decay':     round(decay_term, 5),
                'exogenous': round(exogenous_term, 5),
                'network':   round(network_term, 5),
                'jump':      round(jump_term, 5),
                'reversed':  is_reversal,
            }
        rows_to_persist.append((
            MARKET_ID, fac, as_of,
            json.dumps(topic_sentiments),
            float(oil_share),
            len(news),
            json.dumps(contribution_breakdown),
            CLASSIFIER_VERSION,
        ))

    if recompute:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                DELETE FROM gold.facility_sentiment_daily
                WHERE market_id=%s AND as_of_date=%s AND classifier_version=%s
            """, (MARKET_ID, as_of, CLASSIFIER_VERSION))
            conn.commit()

    sql = """
        INSERT INTO gold.facility_sentiment_daily
            (market_id, facility_id, as_of_date, topic_sentiments,
             oil_share, news_count, contribution_breakdown,
             classifier_version, computed_at)
        VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s::jsonb, %s, NOW())
        ON CONFLICT (market_id, facility_id, as_of_date, classifier_version)
        DO UPDATE SET
            topic_sentiments       = EXCLUDED.topic_sentiments,
            oil_share              = EXCLUDED.oil_share,
            news_count             = EXCLUDED.news_count,
            contribution_breakdown = EXCLUDED.contribution_breakdown,
            computed_at            = NOW()
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.executemany(sql, rows_to_persist)
        conn.commit()
    if n_reversals:
        logger.info(f'  reversals fired this run: {n_reversals}')
    return len(rows_to_persist)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--as-of', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                    help='Single date to compute (default: yesterday UTC)')
    ap.add_argument('--start', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                    help='Backfill from this date forward')
    ap.add_argument('--end', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                    help='Backfill end date (inclusive). Default: today UTC.')
    ap.add_argument('--recompute', action='store_true',
                    help='Wipe + redo even if rows already exist for the date')
    args = ap.parse_args()

    if args.start:
        end = args.end or datetime.now(timezone.utc).date()
        cur_d = args.start
        total = 0
        while cur_d <= end:
            total += update_for_date(cur_d, recompute=args.recompute)
            cur_d += timedelta(days=1)
        logger.info(f'Backfill complete. {total} rows persisted across '
                    f'{(end - args.start).days + 1} days.')
    else:
        as_of = args.as_of or (datetime.now(timezone.utc).date() - timedelta(days=1))
        n = update_for_date(as_of, recompute=args.recompute)
        logger.info(f'Persisted {n} rows for {as_of}.')


if __name__ == '__main__':
    main()
