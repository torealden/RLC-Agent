"""
Consolidate multi-run chart annotation extractions into consensus events.

For each (source_file_hash, page_number), pulls all bronze rows across
extractor_versions (v1-r1, v1-r2, v1-r3 ...), clusters annotations that
refer to the same event (same approximate month + similar verbatim text),
and writes a consensus row to silver.market_event_consensus.

Consensus rules:
  - Group by (calendar month of approximate_date)
  - Within month, cluster annotations by fuzzy text match (difflib ratio >= 0.6)
  - For each cluster:
      consensus_text     = most common verbatim across cluster (longest if tied)
      consensus_polarity = median of polarities across cluster
      consensus_intensity = median of intensities
      n_runs_with_event  = count of distinct extractor_versions in cluster
      n_runs_total       = total extractor_versions for this page
      polarity_stdev     = stdev across cluster (high = runs disagreed)
  - Filter cluster to "real" annotations: position_on_chart='diagonal' only
    (top/bottom strip numbers are not events; they're report data)

Usage:
    python -m scripts.consolidate_chart_annotations
    python -m scripts.consolidate_chart_annotations --source-file '%soy_2.pdf'
"""
from __future__ import annotations

import argparse
import logging
import statistics
import sys
from collections import defaultdict
from datetime import date
from difflib import SequenceMatcher

from dotenv import load_dotenv
load_dotenv()

import psycopg2.extras
from src.services.database.db_config import get_connection


logger = logging.getLogger('consolidate_chart_annotations')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


CONSOLIDATOR_VERSION = 'v1'
MARKET_ID = 'us_oilseed_crush'
TEXT_SIMILARITY_THRESHOLD = 0.6


def fetch_bronze_rows(source_file_pattern: str | None = None):
    """Return all diagonal annotations grouped by (file_hash, page)."""
    sql = """
        SELECT id, source_file, source_file_hash, page_number,
               chart_contract, chart_commodity,
               annotation_index, verbatim_text, position_on_chart,
               approximate_date, approximate_date_label,
               extractor_model, extractor_version, raw_response
        FROM bronze.handwritten_chart_annotation
        WHERE position_on_chart = 'diagonal'
    """
    params = []
    if source_file_pattern:
        sql += ' AND source_file LIKE %s'
        params.append(source_file_pattern)
    sql += ' ORDER BY source_file_hash, page_number, extractor_version, annotation_index'

    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        rows = cur.fetchall()

    grouped = defaultdict(list)
    for r in rows:
        grouped[(r['source_file_hash'], r['page_number'])].append(dict(r))
    return grouped


def text_sim(a: str, b: str) -> float:
    return SequenceMatcher(None, (a or '').lower(), (b or '').lower()).ratio()


def parse_polarity_intensity(raw_response: dict, verbatim: str) -> tuple[float | None, float | None, float | None, str]:
    """Extract polarity, intensity, confidence, topic for an annotation by
    matching its verbatim text inside the raw LLM response."""
    if not raw_response or 'annotations' not in raw_response:
        return None, None, None, 'other'
    best = None
    best_sim = 0
    for ann in raw_response['annotations']:
        sim = text_sim(verbatim, ann.get('verbatim_text', ''))
        if sim > best_sim:
            best_sim = sim
            best = ann
    if best is None or best_sim < 0.7:
        return None, None, None, 'other'
    return (
        best.get('estimated_polarity'),
        best.get('estimated_intensity'),
        best.get('confidence'),
        best.get('estimated_topic') or 'other',
    )


def cluster_annotations(rows: list[dict]) -> list[list[dict]]:
    """Group annotations into clusters where each cluster represents one
    real-world event seen across multiple runs.

    Strategy: bucket by calendar month, then within each month do greedy
    text-similarity merging. Annotations with no date land in a 'no_date'
    bucket and only cluster on text similarity.
    """
    by_month = defaultdict(list)
    for r in rows:
        d = r.get('approximate_date')
        key = (d.year, d.month) if d else ('no_date',)
        by_month[key].append(r)

    clusters = []
    for month_key, month_rows in by_month.items():
        used = set()
        for i, r in enumerate(month_rows):
            if i in used:
                continue
            cluster = [r]
            used.add(i)
            for j, r2 in enumerate(month_rows[i + 1:], start=i + 1):
                if j in used:
                    continue
                if text_sim(r['verbatim_text'], r2['verbatim_text']) >= TEXT_SIMILARITY_THRESHOLD:
                    cluster.append(r2)
                    used.add(j)
            clusters.append(cluster)
    return clusters


def consolidate_chart(file_hash: str, page: int, rows: list[dict]) -> int:
    """Process one chart page. Returns number of consensus rows written."""
    if not rows:
        return 0
    n_runs_total = len({r['extractor_version'] for r in rows})
    contract = next((r['chart_contract'] for r in rows if r['chart_contract']), None)
    commodity = next((r['chart_commodity'] for r in rows if r['chart_commodity']), None)

    clusters = cluster_annotations(rows)
    consensus_rows = []
    for cluster in clusters:
        # Pull polarity/intensity/topic from each row's raw_response
        polarities, intensities, confidences, topics = [], [], [], []
        for c in cluster:
            p, i, conf, t = parse_polarity_intensity(c['raw_response'], c['verbatim_text'])
            if p is not None: polarities.append(float(p))
            if i is not None: intensities.append(float(i))
            if conf is not None: confidences.append(float(conf))
            topics.append(t)
        if not polarities:  # no useful data
            continue

        from collections import Counter
        topic_counts = Counter(topics)
        consensus_topic, _ = topic_counts.most_common(1)[0]

        text_counts = Counter((c['verbatim_text'] or '').strip() for c in cluster)
        consensus_text = text_counts.most_common(1)[0][0]

        # Use median date as consensus date
        dates = [c['approximate_date'] for c in cluster if c['approximate_date']]
        consensus_date = sorted(dates)[len(dates) // 2] if dates else None

        n_runs_with_event = len({c['extractor_version'] for c in cluster})

        consensus_rows.append({
            'consensus_date': consensus_date,
            'consensus_text': consensus_text,
            'topic_key': consensus_topic,
            'consensus_polarity': statistics.median(polarities),
            'consensus_intensity': statistics.median(intensities) if intensities else None,
            'n_runs_total': n_runs_total,
            'n_runs_with_event': n_runs_with_event,
            'median_confidence': statistics.median(confidences) if confidences else None,
            'polarity_stdev': statistics.stdev(polarities) if len(polarities) > 1 else 0.0,
            'bronze_ids': [c['id'] for c in cluster],
            'contract': contract,
            'commodity': commodity,
        })

    # Persist
    with get_connection() as conn:
        cur = conn.cursor()
        for cr in consensus_rows:
            cur.execute("""
                INSERT INTO silver.market_event_consensus
                    (source_file_hash, page_number, chart_contract,
                     chart_commodity, market_id,
                     consensus_date, consensus_text, topic_key,
                     consensus_polarity, consensus_intensity,
                     n_runs_total, n_runs_with_event,
                     median_confidence, polarity_stdev,
                     bronze_ids, consolidator_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_file_hash, page_number, consensus_date,
                             consensus_text, consolidator_version)
                DO UPDATE SET
                    chart_contract       = EXCLUDED.chart_contract,
                    chart_commodity      = EXCLUDED.chart_commodity,
                    topic_key            = EXCLUDED.topic_key,
                    consensus_polarity   = EXCLUDED.consensus_polarity,
                    consensus_intensity  = EXCLUDED.consensus_intensity,
                    n_runs_total         = EXCLUDED.n_runs_total,
                    n_runs_with_event    = EXCLUDED.n_runs_with_event,
                    median_confidence    = EXCLUDED.median_confidence,
                    polarity_stdev       = EXCLUDED.polarity_stdev,
                    bronze_ids           = EXCLUDED.bronze_ids,
                    consolidated_at      = NOW()
            """, (
                file_hash, page, cr['contract'], cr['commodity'], MARKET_ID,
                cr['consensus_date'], cr['consensus_text'][:500], cr['topic_key'],
                cr['consensus_polarity'], cr['consensus_intensity'],
                cr['n_runs_total'], cr['n_runs_with_event'],
                cr['median_confidence'], cr['polarity_stdev'],
                cr['bronze_ids'], CONSOLIDATOR_VERSION,
            ))
        conn.commit()
    return len(consensus_rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--source-file', help='Filter to a specific source_file LIKE pattern '
                                          '(e.g., %%soy_2.pdf). Otherwise consolidates all.')
    args = ap.parse_args()

    grouped = fetch_bronze_rows(args.source_file)
    logger.info(f'Found {len(grouped)} (file, page) groups to consolidate.')

    total_consensus = 0
    for (file_hash, page), rows in grouped.items():
        n = consolidate_chart(file_hash, page, rows)
        total_consensus += n
        sample = rows[0]['source_file'].split('/')[-1].split('\\')[-1]
        logger.info(f'  {sample} page {page}: {len(rows)} bronze rows -> {n} consensus events')

    logger.info(f'Done. {total_consensus} consensus rows in silver.market_event_consensus.')


if __name__ == '__main__':
    main()
