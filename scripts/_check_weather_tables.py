"""One-off: count rows + most-recent timestamp for every weather table."""
from dotenv import load_dotenv
load_dotenv()
import psycopg2.extras
from src.services.database.db_config import get_connection

# (table, candidate timestamp columns to try in order)
TABLES = [
    ('bronze.weather_alerts_raw', ['collected_at', 'created_at']),
    ('bronze.weather_daily_brief', ['created_at', 'as_of_date']),
    ('bronze.weather_email_extract', ['email_date', 'received_date', 'sent_at']),
    ('bronze.weather_forecast_run', ['run_at', 'forecast_run_time']),
    ('bronze.weather_raw', ['collected_at']),
    ('core.llm_briefing', ['briefing_time', 'as_of']),
    ('silver.weather_alert', []),
    ('silver.weather_forecast_daily', ['forecast_date']),
    ('silver.weather_observation', ['observation_date']),
    ('public.weather_location', []),
]


def get_cols(schema: str, name: str) -> list[str]:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (schema, name),
        )
        rows = cur.fetchall()
    # Tolerate either cursor type
    return [r['column_name'] if isinstance(r, dict) else r[0] for r in rows]


def first_present(cands: list[str], cols: list[str]) -> str | None:
    for c in cands:
        if c in cols:
            return c
    return None


for tbl, ts_cands in TABLES:
    schema, name = tbl.split('.')
    try:
        cols = get_cols(schema, name)
    except Exception as e:
        print(f'  {tbl:42s} ERR getting cols: {e}')
        continue

    ts_col = first_present(ts_cands, cols)
    sql = f"SELECT COUNT(*) AS n, {('MAX(' + ts_col + ')') if ts_col else 'NULL'} AS last FROM {tbl}"
    try:
        with get_connection() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql)
            r = cur.fetchone()
            n = r['n']
            last = r['last']
        suffix = f"  last({ts_col})={last}" if ts_col else "  (no ts col tried)"
        print(f'  {tbl:42s} {n:>12,} rows{suffix}')
    except Exception as e:
        print(f'  {tbl:42s} ERR query: {e}  (cols sample: {cols[:6]})')
