"""
Market Field Layer 1, step 4: News article collector.

Pulls articles from registered sources in reference.news_source, dedups by
url-hash, and upserts into bronze.news_article. Idempotent — running
multiple times never duplicates rows. Tracks last_fetched_at per source.

Sources:
  rss     — pulled via feedparser
  gdelt   — pulled via GDELT 2.0 Article List API

PoC scope: title + snippet only. Full article body is NOT fetched yet
(would require per-source HTML scraping with separate logic per outlet).
The classifier will run on title + snippet + source-supplied summary,
which is sufficient for topic detection and sentiment polarity.

Usage:
    python -m scripts.collect_news_articles                # all active sources
    python -m scripts.collect_news_articles --source agweek
    python -m scripts.collect_news_articles --hours 48     # GDELT lookback window
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from dotenv import load_dotenv
load_dotenv()

import psycopg2.extras
import requests

try:
    import feedparser
except ImportError:
    sys.exit("feedparser not installed. Run: pip install feedparser")

from src.services.database.db_config import get_connection


logger = logging.getLogger('collect_news_articles')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

USER_AGENT = ('Round Lakes Commodities Market Field collector '
              '(commodity research; contact tore@roundlakescommodities.com)')

# GDELT broad ag/biofuel query — the AND/OR/IF syntax follows GDELT v2 docs
GDELT_QUERY = ('(soybean OR "soybean oil" OR "soybean meal" OR ethanol OR '
               'biodiesel OR "renewable diesel" OR "sustainable aviation fuel" OR '
               'crush OR feedstock OR "vegetable oil" OR "yellow grease" OR '
               '"used cooking oil" OR "choice white grease" OR tallow) '
               'sourcecountry:US sourcelang:eng')


@dataclass
class Article:
    source_type: str
    source_name: str
    article_url: str
    title: str
    body: Optional[str]
    snippet: Optional[str]
    published_at: Optional[datetime]
    raw_metadata: dict


def normalise_url(url: str) -> str:
    """Strip common tracking parameters and trailing slash for stable hashing."""
    url = re.sub(r'\?(utm_|gclid|fbclid|mc_cid|mc_eid).*', '', url, count=1)
    url = re.sub(r'#.*$', '', url)
    return url.rstrip('/').lower()


def article_hash(url: str) -> str:
    return hashlib.sha256(normalise_url(url).encode()).hexdigest()


def parse_published(entry: dict) -> Optional[datetime]:
    """Best-effort parse of published timestamp from feedparser entry."""
    for key in ('published_parsed', 'updated_parsed'):
        ts = entry.get(key)
        if ts:
            try:
                return datetime(*ts[:6], tzinfo=timezone.utc)
            except (ValueError, TypeError):
                continue
    return None


def fetch_rss(source: dict) -> Iterable[Article]:
    """Pull a single RSS feed; yield Article objects."""
    url = source['url_template']
    try:
        resp = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f'  RSS fetch failed for {source["source_name"]}: {e}')
        return

    feed = feedparser.parse(resp.content)
    if feed.bozo and feed.entries == []:
        logger.warning(f'  Feed parse error for {source["source_name"]}: '
                       f'{getattr(feed, "bozo_exception", "unknown")}')
        return

    for entry in feed.entries:
        article_url = entry.get('link') or ''
        if not article_url:
            continue
        title = entry.get('title') or ''
        snippet = entry.get('summary') or entry.get('description') or ''
        # Strip HTML tags from snippet for storage
        snippet_clean = re.sub(r'<[^>]+>', ' ', snippet)
        snippet_clean = re.sub(r'\s+', ' ', snippet_clean).strip()[:2000]
        yield Article(
            source_type='rss',
            source_name=source['source_name'],
            article_url=article_url,
            title=title.strip()[:1000],
            body=None,                  # full body fetch deferred
            snippet=snippet_clean,
            published_at=parse_published(entry),
            raw_metadata={
                'rss_id': entry.get('id'),
                'author': entry.get('author'),
                'tags': [t.get('term') for t in entry.get('tags', [])
                         if isinstance(t, dict)],
            },
        )


def fetch_gdelt(source: dict, hours_back: int = 24) -> Iterable[Article]:
    """Query GDELT 2.0 Article List for ag/biofuel news."""
    base_url = ('https://api.gdeltproject.org/api/v2/doc/doc?'
                'query={q}&mode=ArtList&format=json&maxrecords=250&'
                'startdatetime={start}&enddatetime={end}')
    end = datetime.utcnow()
    start = end - timedelta(hours=hours_back)
    url = base_url.format(
        q=requests.utils.quote(GDELT_QUERY),
        start=start.strftime('%Y%m%d%H%M%S'),
        end=end.strftime('%Y%m%d%H%M%S'),
    )

    try:
        resp = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        logger.error(f'  GDELT fetch failed: {e}')
        return

    for art in data.get('articles', []):
        article_url = art.get('url') or ''
        if not article_url:
            continue
        seen = art.get('seendate', '')
        published_at = None
        if seen:
            try:
                published_at = datetime.strptime(seen, '%Y%m%dT%H%M%SZ').replace(
                    tzinfo=timezone.utc)
            except ValueError:
                pass
        yield Article(
            source_type='gdelt',
            source_name='gdelt',
            article_url=article_url,
            title=(art.get('title') or '').strip()[:1000],
            body=None,
            snippet=art.get('seendate') or '',  # GDELT doesn't return summary
            published_at=published_at,
            raw_metadata={
                'domain': art.get('domain'),
                'language': art.get('language'),
                'sourcecountry': art.get('sourcecountry'),
                'tone': art.get('tone'),     # GDELT pre-computed sentiment, useful prior
                'socialimage': art.get('socialimage'),
            },
        )


def upsert_articles(articles: list[Article]) -> int:
    """Upsert articles to bronze.news_article. Returns count of NEW rows
    (existing articles via ON CONFLICT DO NOTHING are not counted)."""
    if not articles:
        return 0
    sql = """
        INSERT INTO bronze.news_article
            (source_type, source_name, article_url, article_id_hash,
             title, body, snippet, published_at, fetched_at, language,
             raw_metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'en', %s::jsonb)
        ON CONFLICT (article_id_hash) DO NOTHING
        RETURNING id
    """
    import json
    n_new = 0
    with get_connection() as conn:
        cur = conn.cursor()
        for a in articles:
            cur.execute(sql, (
                a.source_type, a.source_name, a.article_url,
                article_hash(a.article_url), a.title, a.body, a.snippet,
                a.published_at, json.dumps(a.raw_metadata),
            ))
            if cur.fetchone():
                n_new += 1
        conn.commit()
    return n_new


def update_source_fetched_at(source_name: str) -> None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE reference.news_source SET last_fetched_at=NOW() "
            "WHERE source_name=%s",
            (source_name,),
        )
        conn.commit()


def get_active_sources(filter_name: Optional[str] = None) -> list[dict]:
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if filter_name:
            cur.execute(
                "SELECT * FROM reference.news_source "
                "WHERE source_name=%s AND is_active=TRUE",
                (filter_name,),
            )
        else:
            cur.execute(
                "SELECT * FROM reference.news_source WHERE is_active=TRUE "
                "ORDER BY source_name"
            )
        return [dict(r) for r in cur.fetchall()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--source', help='Limit to this source_name')
    ap.add_argument('--hours', type=int, default=24,
                    help='Lookback window for GDELT (default 24)')
    args = ap.parse_args()

    sources = get_active_sources(args.source)
    if not sources:
        sys.exit(f'No active sources found{" matching " + args.source if args.source else ""}.')

    logger.info(f'Fetching from {len(sources)} active source(s)')
    grand_total = 0
    grand_new = 0
    for s in sources:
        logger.info(f'  {s["source_name"]} ({s["source_type"]})')
        articles = list(
            fetch_rss(s) if s['source_type'] == 'rss'
            else fetch_gdelt(s, args.hours) if s['source_type'] == 'gdelt'
            else []
        )
        n_new = upsert_articles(articles)
        update_source_fetched_at(s['source_name'])
        grand_total += len(articles)
        grand_new += n_new
        logger.info(f'    fetched={len(articles)}  new_to_bronze={n_new}')

    logger.info(f'Done. Total fetched: {grand_total}, new in bronze: {grand_new}.')


if __name__ == '__main__':
    main()
