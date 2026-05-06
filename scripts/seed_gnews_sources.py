"""
Seed reference.news_source with one Google News RSS query per
(operator, city) cluster across all canonical IA facilities.

Why per-cluster: Cargill Cedar Rapids East and West are 1mi apart;
Google News can't distinguish them. We deduplicate at the cluster
level — one query "Cargill Cedar Rapids" surfaces stories that the
classifier later tags with both facility_ids when relevant.

Source type: 'rss' (Google News returns standard RSS XML). The
existing fetch_rss path in collect_news_articles.py handles this
transparently.

Idempotent: ON CONFLICT (source_name) DO UPDATE — rerunning after a
facility addition adds the new query without disturbing existing rows.

Usage:
    python -m scripts.seed_gnews_sources              # seed all canonical IA
    python -m scripts.seed_gnews_sources --dry-run    # show queries, do not write
"""
from __future__ import annotations

import argparse
import re
import urllib.parse
from collections import defaultdict
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

import psycopg2.extras
from src.services.database.db_config import get_connection


GNEWS_BASE = ('https://news.google.com/rss/search?q={query}'
              '&hl=en-US&gl=US&ceid=US:en')


# Operator name normalisation: collapse parent-vs-subsidiary naming so
# the query uses the form that Google News will best resolve.
OPERATOR_FOR_QUERY = {
    'AGP': 'AGP',
    'Ag Processing': 'AGP',
    'Ag Processing Inc.': 'AGP',
    'Cargill': 'Cargill',
    'Cargill Cedar Rapids': 'Cargill',
    'Cargill Cedar Rapids East': 'Cargill',
    'Cargill Cedar Rapids West': 'Cargill',
    'ADM': 'ADM',
    'Bunge': 'Bunge',
    'Platinum Crush (N Bowdish)': 'Platinum Crush',
    'Shell Rock Soy Processing': 'Shell Rock Soy Processing',
    'White River Nutrition': 'White River',
}


def slugify(s: str) -> str:
    """Make a stable, lowercased identifier from a free-text label."""
    s = s.lower().strip()
    s = re.sub(r'[^a-z0-9]+', '_', s)
    return s.strip('_')


def get_canonical_geocoded_facilities():
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT facility_id, operator, city
            FROM reference.oilseed_crush_facilities
            WHERE state='IA' AND is_canonical=TRUE
              AND lat IS NOT NULL AND lat <> 0
            ORDER BY operator, city
        """)
        return [dict(r) for r in cur.fetchall()]


def normalise_city(city: str) -> str:
    """Strip directional suffixes so 'Cedar Rapids East' and 'Cedar Rapids West'
    cluster together — Google News can't distinguish 1mi-apart plants."""
    if not city:
        return ''
    return re.sub(r'\s+(East|West|North|South)$', '', city.strip(),
                  flags=re.IGNORECASE)


def build_clusters(facilities):
    """Group facilities by (normalised_operator, normalised_city)."""
    clusters = defaultdict(list)
    for f in facilities:
        op = OPERATOR_FOR_QUERY.get(f['operator'], f['operator'] or '')
        city = normalise_city(f['city'] or '')
        if not op or not city:
            continue
        clusters[(op, city)].append(f['facility_id'])
    return clusters


def build_source_rows(clusters):
    """Convert clusters to news_source rows."""
    rows = []
    for (op, city), facility_ids in sorted(clusters.items()):
        query = f'"{op}" "{city}" Iowa'
        url = GNEWS_BASE.format(query=urllib.parse.quote(query))
        source_name = f'gnews_{slugify(op)}_{slugify(city)}'
        rows.append({
            'source_name': source_name,
            'source_type': 'rss',
            'url_template': url,
            'polling_frequency_minutes': 720,    # 12 hours; local news is bursty
            'default_locality': 'local',
            'default_topic_focus': ['competitor_activity', 'policy_state_local'],
            'source_weight': 0.85,    # slightly under 1.0; Google News mixes outlet quality
            'facility_ids': facility_ids,        # for log/inspection only — not a column
        })
    return rows


def upsert(rows: list[dict], dry_run: bool):
    if dry_run:
        print(f'\nDRY-RUN: {len(rows)} sources would be seeded:\n')
        for r in rows:
            ids = ', '.join(r['facility_ids'])
            print(f'  {r["source_name"]:55s} -> {ids}')
            print(f'    {r["url_template"]}')
        return 0
    sql = """
        INSERT INTO reference.news_source
            (source_name, source_type, url_template,
             polling_frequency_minutes, default_locality,
             default_topic_focus, source_weight, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
        ON CONFLICT (source_name) DO UPDATE SET
            source_type = EXCLUDED.source_type,
            url_template = EXCLUDED.url_template,
            polling_frequency_minutes = EXCLUDED.polling_frequency_minutes,
            default_locality = EXCLUDED.default_locality,
            default_topic_focus = EXCLUDED.default_topic_focus,
            source_weight = EXCLUDED.source_weight,
            is_active = TRUE
    """
    with get_connection() as conn:
        cur = conn.cursor()
        for r in rows:
            cur.execute(sql, (
                r['source_name'], r['source_type'], r['url_template'],
                r['polling_frequency_minutes'], r['default_locality'],
                r['default_topic_focus'], r['source_weight'],
            ))
        conn.commit()
    return len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    facilities = get_canonical_geocoded_facilities()
    clusters = build_clusters(facilities)
    rows = build_source_rows(clusters)
    print(f'Built {len(rows)} Google News query rows from '
          f'{len(facilities)} canonical IA facilities.')
    n = upsert(rows, args.dry_run)
    if not args.dry_run:
        print(f'Persisted {n} rows to reference.news_source.')


if __name__ == '__main__':
    main()
