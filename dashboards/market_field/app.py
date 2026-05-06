"""
Market Field viewer — sentiment trajectories per facility, news driving them.

Three panels:
  1. Cross-facility heatmap on a chosen date
  2. Time series for a selected facility, all topics
  3. Recent classified articles tagged for that facility, with their topic
     score contributions

Launch:
    streamlit run dashboards/market_field/app.py

Reads from gold.facility_sentiment_daily, silver.news_classified, and
bronze.news_article. Read-only; no writes.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / '.env')

import psycopg2.extras
import pandas as pd
import streamlit as st

from src.services.database.db_config import get_connection


st.set_page_config(page_title='Market Field — Iowa Crush', layout='wide')

MARKET_ID = 'us_oilseed_crush'
CV = 'mf-v1'

TOPIC_ORDER = [
    'weather', 'soybean_supply',
    'veg_oil_demand', 'meal_livestock_demand',
    'policy_federal', 'policy_state_local', 'policy_industry',
    'competitor_activity',
]


@st.cache_data(ttl=300)
def get_dates() -> list[date]:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT as_of_date FROM gold.facility_sentiment_daily "
            "WHERE market_id=%s AND classifier_version=%s "
            "ORDER BY as_of_date DESC",
            (MARKET_ID, CV),
        )
        rows = cur.fetchall()
    return [r[0] if not isinstance(r, dict) else r['as_of_date'] for r in rows]


@st.cache_data(ttl=300)
def get_facilities() -> list[str]:
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT DISTINCT facility_id FROM gold.facility_sentiment_daily "
            "WHERE market_id=%s ORDER BY facility_id",
            (MARKET_ID,),
        )
        return [r['facility_id'] for r in cur.fetchall()]


@st.cache_data(ttl=300)
def heatmap_for_date(d: date) -> pd.DataFrame:
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT facility_id, topic_sentiments "
            "FROM gold.facility_sentiment_daily "
            "WHERE market_id=%s AND classifier_version=%s AND as_of_date=%s "
            "ORDER BY facility_id",
            (MARKET_ID, CV, d),
        )
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    data = []
    for r in rows:
        row = {'facility_id': r['facility_id']}
        for t in TOPIC_ORDER:
            row[t] = float(r['topic_sentiments'].get(t, 0))
        data.append(row)
    return pd.DataFrame(data).set_index('facility_id')


@st.cache_data(ttl=300)
def series_for_facility(facility_id: str) -> pd.DataFrame:
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT as_of_date, topic_sentiments, news_count "
            "FROM gold.facility_sentiment_daily "
            "WHERE market_id=%s AND classifier_version=%s AND facility_id=%s "
            "ORDER BY as_of_date",
            (MARKET_ID, CV, facility_id),
        )
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    data = []
    for r in rows:
        row = {'date': r['as_of_date'], 'news_count': r['news_count']}
        for t in TOPIC_ORDER:
            row[t] = float(r['topic_sentiments'].get(t, 0))
        data.append(row)
    return pd.DataFrame(data).set_index('date')


@st.cache_data(ttl=300)
def recent_articles_for_facility(facility_id: str, n: int = 20) -> pd.DataFrame:
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT n.title, n.source_name, n.published_at,
                      c.locality, c.confidence_score, c.topic_scores
               FROM silver.news_classified c
               JOIN bronze.news_article n ON n.id = c.news_article_id
               WHERE c.market_id=%s AND c.classifier_version=%s
                 AND %s = ANY(c.facility_relevance_keys)
               ORDER BY n.published_at DESC NULLS LAST
               LIMIT %s""",
            (MARKET_ID, CV, facility_id, n),
        )
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    out = []
    for r in rows:
        scores = r['topic_scores'] or {}
        # Pick the topic with the largest |polarity * intensity| as the headline driver
        best_topic = ''
        best_mag = 0
        for t, sc in scores.items():
            if isinstance(sc, dict):
                mag = abs(float(sc.get('polarity', 0)) * float(sc.get('intensity', 0)))
                if mag > best_mag:
                    best_mag = mag
                    best_topic = t
        out.append({
            'date': r['published_at'].date() if r['published_at'] else None,
            'source': r['source_name'],
            'locality': r['locality'],
            'top_topic': best_topic,
            'confidence': r['confidence_score'],
            'title': r['title'],
        })
    return pd.DataFrame(out)


# --- UI ---

st.title('🌽 Market Field — Iowa Oilseed Crush')
st.caption('Sentiment dynamics on the facility graph. Read-only viewer.')

dates = get_dates()
facilities = get_facilities()
if not dates or not facilities:
    st.warning('No sentiment data yet. Run scripts/update_facility_sentiment.py first.')
    st.stop()

col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader(f'Cross-facility heatmap')
    pick_date = st.selectbox('Date', dates, index=0)
    df = heatmap_for_date(pick_date)
    if df.empty:
        st.info('No data for that date.')
    else:
        st.dataframe(
            df.style.background_gradient(cmap='RdYlGn', axis=None, vmin=-1, vmax=1)
                    .format('{:+.3f}'),
            width='stretch',
        )

with col_right:
    st.subheader('Pick a facility')
    pick_fac = st.selectbox('Facility', facilities, index=0)
    fac_series = series_for_facility(pick_fac)
    if not fac_series.empty:
        st.metric('Most recent news_count',
                  int(fac_series.iloc[-1]['news_count']))

st.divider()
st.subheader(f'Sentiment trajectory — {pick_fac}')

if not fac_series.empty:
    st.line_chart(fac_series[TOPIC_ORDER])
else:
    st.info('No trajectory data for this facility.')

st.divider()
st.subheader(f'Recent classified articles tagged for {pick_fac}')

articles = recent_articles_for_facility(pick_fac)
if articles.empty:
    st.info(
        'No facility-tagged articles yet. The classifier may not have surfaced '
        'this plant in recent stories — try another facility, or wait for the '
        'next collection run.'
    )
else:
    st.dataframe(articles, width='stretch')

st.caption(
    'Layer 1 of the Market Field. Sentiment values in [-1, +1]; negative = '
    'bearish on that topic, positive = bullish. Cross-facility variation '
    'reflects the network propagation: same news, different reach depending '
    'on edge weights (parent_company, draw_region, industry).'
)
