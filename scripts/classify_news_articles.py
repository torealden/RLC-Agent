"""
Market Field Layer 1, step 5: News article classifier.

For each unclassified article in bronze.news_article, calls Claude to
classify it across the 8 us_oilseed_crush topics, identify locality,
and tag facility relevance, then writes one row to silver.news_classified.

Idempotent on (news_article_id, market_id, classifier_version): re-running
will skip articles already classified for the active version. To re-run a
classification with new logic, bump CLASSIFIER_VERSION below.

Usage:
    python -m scripts.classify_news_articles                # all unclassified
    python -m scripts.classify_news_articles --limit 5      # smoke-test
    python -m scripts.classify_news_articles --dry-run      # show output, don't persist
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

import psycopg2.extras

try:
    import anthropic
except ImportError:
    sys.exit("Run: pip install anthropic")

from src.services.database.db_config import get_connection


logger = logging.getLogger('classify_news_articles')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


MARKET_ID = 'us_oilseed_crush'
CLASSIFIER_VERSION = 'mf-v1'
MODEL = 'claude-sonnet-4-6'   # classification benefits from Sonnet's reasoning over Haiku
MAX_TOKENS = 1500
RATE_LIMIT_SLEEP_S = 0.4   # gentle pacing — anthropic is fine with bursts but rate-limit-friendly anyway


def get_unclassified_articles(limit: Optional[int] = None) -> list[dict]:
    """Pull articles in bronze.news_article that have no row in silver.news_classified
    for the current market+classifier version."""
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        sql = """
            SELECT n.id, n.source_name, n.title, n.snippet, n.published_at,
                   n.article_url, n.raw_metadata
            FROM bronze.news_article n
            LEFT JOIN silver.news_classified c
              ON c.news_article_id = n.id
             AND c.market_id = %s
             AND c.classifier_version = %s
            WHERE c.id IS NULL
            ORDER BY n.published_at DESC NULLS LAST
        """
        params = [MARKET_ID, CLASSIFIER_VERSION]
        if limit is not None:
            sql += " LIMIT %s"
            params.append(limit)
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def get_facility_keys() -> list[dict]:
    """Pull canonical IA facilities for the facility relevance hint."""
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT facility_id, name, operator, city
            FROM reference.oilseed_crush_facilities
            WHERE state='IA' AND is_canonical=TRUE
            ORDER BY facility_id
        """)
        return [dict(r) for r in cur.fetchall()]


def get_topic_taxonomy() -> list[dict]:
    """Pull the active topic taxonomy from the reference table."""
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT topic_key, topic_name, category, description
            FROM reference.market_topic_taxonomy
            WHERE market_id=%s AND is_active=TRUE
            ORDER BY sort_order
        """, (MARKET_ID,))
        return [dict(r) for r in cur.fetchall()]


def build_system_prompt(topics: list[dict], facilities: list[dict]) -> str:
    """Construct the system prompt with topic + facility context baked in."""
    topics_block = '\n'.join(
        f'  - {t["topic_key"]} ({t["category"]}): {t["topic_name"]}\n'
        f'      {t["description"]}'
        for t in topics
    )
    facilities_block = '\n'.join(
        f'  - {f["facility_id"]}: {f["name"] or f["operator"]} ({f["city"]})'
        for f in facilities
    )
    return f"""You are a senior commodity-market analyst at Round Lakes Commodities, classifying news articles for an Iowa-oilseed-crush market intelligence system.

Your job: for each article, return a JSON object scoring how strongly it touches each of 8 topics, identifying its geographic scope, and tagging any specific facilities mentioned.

# 8 topics (us_oilseed_crush market)

{topics_block}

# Iowa canonical facilities (for facility_relevance_keys)

{facilities_block}

# Scoring rules

For each topic, return TWO numbers:
  polarity:  in [-1.0, +1.0]. -1 = very bearish/negative for the topic; +1 = very bullish/positive; 0 = neutral or not discussed.
  intensity: in [0.0, 1.0].   0 = topic not discussed at all; 1 = article is entirely about this topic with significant new information.

Most articles touch 1-3 topics. Set intensity=0 for topics not discussed, regardless of polarity.

Polarity examples:
  "RFS rule cuts D4 RIN volumes" -> policy_federal: polarity=-0.7, intensity=0.9 (bearish for biofuel demand)
  "AGP announces Sergeant Bluff expansion" -> competitor_activity: polarity=+0.6, intensity=0.8 (bullish, capacity growth)
  "Hog herd expansion lifts Q3 meal demand outlook" -> meal_livestock_demand: polarity=+0.5, intensity=0.6
  "Drought conditions worsen in Iowa" -> weather: polarity=-0.6, intensity=0.7 (bearish for supply)

# Locality

  national  - federal policy, broad market commentary, USDA reports, macro events
  regional  - Midwest, Corn Belt, multi-state focus, Iowa generally
  local     - specific Iowa county, town, or sub-region
  facility  - specific named plant or operator action

# Facility relevance

If the article mentions a specific Iowa facility from the list above, include its facility_id in facility_relevance_keys. Empty list otherwise. Match by company + city — e.g., "Cargill's Cedar Rapids plant" tags both ia.cargill_cedar_rapids_east and ia.cargill_cedar_rapids_west unless one is specifically distinguished.

# Output format

Return ONLY a JSON object, no preamble, no markdown fences:

{{
  "topics": {{
    "weather": {{"polarity": -0.0, "intensity": 0.0}},
    "soybean_supply": {{"polarity": 0.0, "intensity": 0.0}},
    "veg_oil_demand": {{"polarity": 0.0, "intensity": 0.0}},
    "meal_livestock_demand": {{"polarity": 0.0, "intensity": 0.0}},
    "policy_federal": {{"polarity": 0.0, "intensity": 0.0}},
    "policy_state_local": {{"polarity": 0.0, "intensity": 0.0}},
    "policy_industry": {{"polarity": 0.0, "intensity": 0.0}},
    "competitor_activity": {{"polarity": 0.0, "intensity": 0.0}}
  }},
  "locality": "national",
  "facility_relevance_keys": [],
  "confidence_score": 0.85,
  "reasoning": "One short sentence on why."
}}
"""


def classify_one(client, system_prompt: str, article: dict) -> Optional[dict]:
    """Send one article to Claude, return parsed classification dict."""
    user_text = (
        f'Source: {article["source_name"]}\n'
        f'Published: {article.get("published_at", "unknown")}\n'
        f'Title: {article["title"]}\n'
        f'\nSnippet: {article.get("snippet") or "(no snippet available)"}\n'
    )
    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=system_prompt,
            messages=[{'role': 'user', 'content': user_text}],
        )
    except anthropic.APIError as e:
        logger.error(f'  Claude API error on article {article["id"]}: {e}')
        return None

    text = resp.content[0].text.strip() if resp.content else ''
    # Some responses prepend stray text; try to extract the first JSON object
    if not text.startswith('{'):
        start = text.find('{')
        end = text.rfind('}')
        if start >= 0 and end > start:
            text = text[start:end + 1]

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        logger.warning(f'  JSON parse failed for article {article["id"]}: {e}')
        logger.warning(f'    raw response (first 200 chars): {text[:200]}')
        return None

    # attach token counts for cost tracking
    parsed['_usage'] = {
        'input_tokens': resp.usage.input_tokens,
        'output_tokens': resp.usage.output_tokens,
    }
    return parsed


def persist(article_id: int, parsed: dict) -> None:
    """Write one classified row to silver.news_classified."""
    sql = """
        INSERT INTO silver.news_classified
            (news_article_id, market_id, topic_scores, locality,
             facility_relevance_keys, confidence_score,
             classifier_version, llm_model, llm_prompt_tokens,
             llm_completion_tokens, classified_at)
        VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (news_article_id, market_id, classifier_version) DO UPDATE SET
            topic_scores            = EXCLUDED.topic_scores,
            locality                = EXCLUDED.locality,
            facility_relevance_keys = EXCLUDED.facility_relevance_keys,
            confidence_score        = EXCLUDED.confidence_score,
            llm_model               = EXCLUDED.llm_model,
            llm_prompt_tokens       = EXCLUDED.llm_prompt_tokens,
            llm_completion_tokens   = EXCLUDED.llm_completion_tokens,
            classified_at           = NOW()
    """
    usage = parsed.get('_usage', {})
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (
            article_id,
            MARKET_ID,
            json.dumps(parsed.get('topics') or {}),
            parsed.get('locality'),
            parsed.get('facility_relevance_keys') or [],
            parsed.get('confidence_score'),
            CLASSIFIER_VERSION,
            MODEL,
            usage.get('input_tokens'),
            usage.get('output_tokens'),
        ))
        conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, help='Only classify N articles')
    ap.add_argument('--dry-run', action='store_true',
                    help='Print classifications, do not persist')
    args = ap.parse_args()

    articles = get_unclassified_articles(args.limit)
    if not articles:
        logger.info('No unclassified articles.')
        return

    topics = get_topic_taxonomy()
    facilities = get_facility_keys()
    if not topics or not facilities:
        sys.exit('Topic taxonomy or facility list empty — check migrations 049 + 052 ran.')

    system_prompt = build_system_prompt(topics, facilities)
    logger.info(f'Classifying {len(articles)} articles with {MODEL}, classifier_version={CLASSIFIER_VERSION}')

    client = anthropic.Anthropic()
    n_ok = n_fail = 0
    total_input_tokens = total_output_tokens = 0

    for i, art in enumerate(articles, 1):
        parsed = classify_one(client, system_prompt, art)
        if parsed is None:
            n_fail += 1
            continue
        usage = parsed.get('_usage', {})
        total_input_tokens += usage.get('input_tokens') or 0
        total_output_tokens += usage.get('output_tokens') or 0

        if args.dry_run:
            print(f'\n--- {art["id"]} ({art["source_name"]}): {art["title"][:80]}')
            print(json.dumps({k: v for k, v in parsed.items() if k != '_usage'},
                             indent=2))
        else:
            try:
                persist(art['id'], parsed)
                n_ok += 1
            except Exception as e:
                logger.error(f'  Persist failed for article {art["id"]}: {e}')
                n_fail += 1

        if i % 10 == 0:
            logger.info(f'  ... {i}/{len(articles)} done')
        time.sleep(RATE_LIMIT_SLEEP_S)

    # Cost estimate (Sonnet 4.5 pricing as of build date: ~$3/M input, $15/M output)
    cost_in = total_input_tokens / 1_000_000 * 3.0
    cost_out = total_output_tokens / 1_000_000 * 15.0
    logger.info(f'Done. ok={n_ok} fail={n_fail}.  '
                f'tokens in={total_input_tokens:,} out={total_output_tokens:,}  '
                f'~${cost_in + cost_out:.3f}')


if __name__ == '__main__':
    main()
