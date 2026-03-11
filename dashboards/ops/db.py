"""Database query helpers for the RLC Operations Dashboard."""

import psycopg2
import psycopg2.extras
import pandas as pd
from sqlalchemy import create_engine, text
from contextlib import contextmanager
import os


_PG_HOST = os.getenv("RLC_PG_HOST", "localhost")
_PG_PORT = os.getenv("RLC_PG_PORT", "5432")
_PG_DB = os.getenv("RLC_PG_DATABASE", "rlc_commodities")
_PG_USER = os.getenv("RLC_PG_USER", "postgres")
_PG_PASS = os.getenv("RLC_PG_PASSWORD", "SoupBoss1")

DB_CONFIG = {"host": _PG_HOST, "port": _PG_PORT, "database": _PG_DB,
             "user": _PG_USER, "password": _PG_PASS}

_ENGINE = create_engine(f"postgresql://{_PG_USER}:{_PG_PASS}@{_PG_HOST}:{_PG_PORT}/{_PG_DB}")


@contextmanager
def get_connection():
    conn = psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()


def query_df(sql: str, params=None) -> pd.DataFrame:
    """Execute SQL and return a DataFrame."""
    with _ENGINE.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


# ---------------------------------------------------------------------------
# Health score inputs
# ---------------------------------------------------------------------------

def get_health_inputs() -> dict:
    """Return counts needed to compute the health score."""
    with get_connection() as conn:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) AS n FROM core.data_freshness WHERE is_overdue = TRUE")
        overdue = cur.fetchone()["n"]

        cur.execute(
            "SELECT COUNT(*) AS n FROM core.collection_status "
            "WHERE status = 'failed' AND run_started_at > NOW() - INTERVAL '24 hours'"
        )
        failed_24h = cur.fetchone()["n"]

        # Stale but not formally overdue: collected, but hours_since > 24 and not overdue
        cur.execute(
            "SELECT COUNT(*) AS n FROM core.data_freshness "
            "WHERE is_overdue = FALSE AND hours_since_collection > 24"
        )
        stale = cur.fetchone()["n"]

        cur.execute("SELECT COUNT(*) AS n FROM core.data_freshness")
        total = cur.fetchone()["n"]

    score = max(0, 100 - (5 * overdue) - (10 * failed_24h) - (2 * stale))
    return {
        "score": score,
        "total": total,
        "ok": total - overdue,
        "overdue": overdue,
        "failed_24h": failed_24h,
        "stale": stale,
    }


# ---------------------------------------------------------------------------
# Data freshness
# ---------------------------------------------------------------------------

def get_data_freshness() -> pd.DataFrame:
    return query_df(
        "SELECT collector_name, display_name, category, "
        "       last_collected, hours_since_collection, "
        "       expected_frequency, last_status, last_row_count, is_overdue "
        "FROM core.data_freshness "
        "ORDER BY is_overdue DESC, hours_since_collection DESC NULLS FIRST"
    )


# ---------------------------------------------------------------------------
# Alerts (unacknowledged warnings/errors)
# ---------------------------------------------------------------------------

def get_active_alerts() -> pd.DataFrame:
    return query_df(
        "SELECT id, event_time, source, event_type, priority, summary, details "
        "FROM core.event_log "
        "WHERE priority <= 2 AND acknowledged = FALSE "
        "ORDER BY event_time DESC LIMIT 20"
    )


# ---------------------------------------------------------------------------
# Recent failures (last 7 days)
# ---------------------------------------------------------------------------

def get_recent_failures() -> pd.DataFrame:
    return query_df(
        "SELECT collector_name, run_started_at, run_finished_at, error_message, "
        "       EXTRACT(EPOCH FROM (run_finished_at - run_started_at))::int AS duration_sec "
        "FROM core.collection_status "
        "WHERE status = 'failed' AND run_started_at > NOW() - INTERVAL '7 days' "
        "ORDER BY run_started_at DESC"
    )


# ---------------------------------------------------------------------------
# Success rate trends (last 30 days)
# ---------------------------------------------------------------------------

def get_daily_run_counts() -> pd.DataFrame:
    return query_df(
        "SELECT run_started_at::date AS run_date, status, COUNT(*) AS n "
        "FROM core.collection_status "
        "WHERE run_started_at > NOW() - INTERVAL '30 days' "
        "GROUP BY run_date, status ORDER BY run_date"
    )


def get_collector_success_rates() -> pd.DataFrame:
    return query_df(
        "SELECT collector_name, "
        "       COUNT(*) AS total_runs, "
        "       COUNT(*) FILTER (WHERE status = 'success') AS successes, "
        "       COUNT(*) FILTER (WHERE status = 'failed') AS failures, "
        "       ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'success') / NULLIF(COUNT(*), 0), 1) AS success_pct "
        "FROM core.collection_status "
        "WHERE run_started_at > NOW() - INTERVAL '30 days' "
        "GROUP BY collector_name ORDER BY collector_name"
    )


# ---------------------------------------------------------------------------
# Dispatcher status
# ---------------------------------------------------------------------------

def is_dispatcher_running() -> bool:
    """Check if the dispatcher PID file exists."""
    pid_path = os.path.join(os.path.dirname(__file__), "..", "..", "scripts", "deployment", "dispatcher.pid")
    return os.path.isfile(pid_path)
