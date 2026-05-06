"""Inspect cross-facility sentiment variation."""
from dotenv import load_dotenv
load_dotenv()
import psycopg2.extras
from src.services.database.db_config import get_connection

with get_connection() as conn:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    for d in ['2026-05-05', '2026-05-06']:
        cur.execute("""
            SELECT facility_id,
                   (topic_sentiments->>'competitor_activity')::numeric AS comp,
                   (topic_sentiments->>'policy_state_local')::numeric AS pol_st,
                   (topic_sentiments->>'veg_oil_demand')::numeric AS vod,
                   (topic_sentiments->>'policy_federal')::numeric AS pol_fed,
                   (topic_sentiments->>'weather')::numeric AS weather
            FROM gold.facility_sentiment_daily
            WHERE as_of_date = %s
            ORDER BY (topic_sentiments->>'competitor_activity')::numeric DESC
        """, (d,))
        rows = cur.fetchall()
        print(f'{d} -- sorted by competitor_activity:')
        print(f'  {"facility":42s} {"comp":>7s} {"pol_st":>7s} {"vod":>7s} {"pol_fed":>7s} {"weather":>7s}')
        for r in rows:
            print(f'  {r["facility_id"]:42s} {r["comp"]:>+7.3f} {r["pol_st"]:>+7.3f} {r["vod"]:>+7.3f} {r["pol_fed"]:>+7.3f} {r["weather"]:>+7.3f}')
        print()
