"""The Feedstock Report — snapshot ETL.

Populates reports.feedstock_issue / feedstock_price_dashboard_snapshot /
feedstock_credit_stack_snapshot from live silver-layer series, applying the
carry-forward facts the renderer needs (staleness itself is renderer-enforced).

Coverage window (ruled 2026-08-06): ends Monday settlement close; snapshots run
Monday evening; publish Tuesday. week_ending column = the Monday close.

Manual entry path (`load_manual_csv`): supervised CSV -> same validation and
snapshot tables as collector data. Manual numbers never enter rendered output
directly — database only.
"""

from __future__ import annotations

import csv
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env')
from src.services.database.db_config import get_connection

from src.reports.feedstock_report.report_config import (
    DASHBOARD_SERIES, CREDIT_INSTRUMENTS, FEEDSTOCK_CODES, FEEDSTOCK_LABELS,
    IFV_LEADERBOARD_CODES, STALE_EXCLUDE_DAYS,
)

logger = logging.getLogger(__name__)


# =============================================================
# Issue lifecycle
# =============================================================

def monday_close_on_or_before(d: date) -> date:
    """The Monday settlement close covering an issue dated d."""
    return d - timedelta(days=(d.weekday() - 0) % 7)


def create_issue(issue_no: int, issue_date: date, free_mode: bool = True,
                 title: Optional[str] = None) -> int:
    week_ending = monday_close_on_or_before(issue_date)
    coverage_start = week_ending - timedelta(days=6)   # the prior Tuesday
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reports.feedstock_issue
                    (issue_number, issue_date, week_ending, coverage_start,
                     status, free_mode, title)
                VALUES (%s, %s, %s, %s, 'draft', %s, %s)
                ON CONFLICT (issue_number) DO UPDATE SET
                    issue_date = EXCLUDED.issue_date,
                    week_ending = EXCLUDED.week_ending,
                    coverage_start = EXCLUDED.coverage_start,
                    free_mode = EXCLUDED.free_mode,
                    updated_at = NOW()
                RETURNING id
            """, (issue_no, issue_date, week_ending, coverage_start,
                  free_mode, title or f'The Feedstock Report — Issue {issue_no}'))
            issue_id = cur.fetchone()['id']
        conn.commit()
    logger.info(f"Issue {issue_no} (id={issue_id}): coverage "
                f"{coverage_start} -> {week_ending} close, free_mode={free_mode}")
    return issue_id


def get_issue(issue_no: int) -> Dict[str, Any]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM reports.feedstock_issue WHERE issue_number = %s",
                        (issue_no,))
            row = cur.fetchone()
    if not row:
        raise SystemExit(f"Issue {issue_no} does not exist — run `issue create` first")
    return dict(row)


def set_issue_status(issue_no: int, status: str, **paths) -> None:
    sets = ['status = %s', 'updated_at = NOW()']
    vals: list = [status]
    for col in ('html_path', 'linkedin_kit_path'):
        if paths.get(col):
            sets.append(f'{col} = %s')
            vals.append(paths[col])
    if status == 'published':
        sets.append('published_at = NOW()')
    vals.append(issue_no)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE reports.feedstock_issue SET {', '.join(sets)} "
                        f"WHERE issue_number = %s", vals)
        conn.commit()


# =============================================================
# Series print fetchers
# =============================================================

def _prints_consolidated(cur, region: str, code: str, source_like: str,
                         start: date, end: date) -> List[Dict]:
    cur.execute("""
        SELECT price_date, AVG(price_per_lb) AS px
        FROM silver.feedstock_prices_consolidated
        WHERE feedstock_code = %s AND region = %s AND source LIKE %s
          AND price_date BETWEEN %s AND %s AND price_per_lb > 0
        GROUP BY price_date ORDER BY price_date
    """, (code, region, source_like + '%', start, end))
    return [{'date': r['price_date'], 'px': float(r['px'])} for r in cur.fetchall()]


def _prints_price_mark(cur, series_key: str, start: date, end: date) -> List[Dict]:
    cur.execute("""
        SELECT obs_date AS price_date, AVG(value) AS px
        FROM silver.price_mark
        WHERE series_key = %s AND obs_date BETWEEN %s AND %s
        GROUP BY obs_date ORDER BY obs_date
    """, (series_key, start, end))
    return [{'date': r['price_date'], 'px': float(r['px'])} for r in cur.fetchall()]


def _fetch_prints(cur, spec: Dict, start: date, end: date) -> List[Dict]:
    if spec['kind'] == 'consolidated':
        return _prints_consolidated(cur, spec['region'], spec['code'],
                                    spec['source_like'], start, end)
    if spec['kind'] == 'price_mark':
        return _prints_price_mark(cur, spec['series_key'], start, end)
    return []


# =============================================================
# Price dashboard snapshot
# =============================================================

def snapshot_prices(issue_no: int) -> int:
    """Build the price-dashboard snapshot for an issue.

    Carry-forward rule (ruled 2026-08-06): if no print falls inside the
    coverage week, the row carries the LAST actual print with its as-of date
    (is_carried_forward=TRUE) and NULL w/w. W/w change computes only between
    actual prints in the current and prior coverage weeks of the SAME series.
    """
    issue = get_issue(issue_no)
    close: date = issue['week_ending']
    start: date = issue['coverage_start'] or (close - timedelta(days=6))
    n = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            for spec in DASHBOARD_SERIES:
                code = spec['code']
                if spec['kind'] == 'gap':
                    cur.execute("""
                        INSERT INTO reports.feedstock_price_dashboard_snapshot
                            (issue_id, product, location, week_ending, feedstock_code,
                             unit, source, is_placeholder)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                        ON CONFLICT (issue_id, product, location) DO UPDATE SET
                            is_placeholder = TRUE, week_ending = EXCLUDED.week_ending
                    """, (issue['id'], FEEDSTOCK_LABELS[code], 'coverage gap',
                          close, code, '', ''))
                    n += 1
                    continue

                scale = spec.get('scale', 1.0)
                # All prints up to close (for carry-forward + ranges)
                hist = _fetch_prints(cur, spec, close - timedelta(weeks=52), close)
                if not hist:
                    logger.warning(f"{code}: no prints at all in trailing 52w — skipping")
                    continue
                last_print = hist[-1]
                in_window = [p for p in hist if start <= p['date'] <= close]
                prior_window = [p for p in hist
                                if start - timedelta(days=7) <= p['date'] <= close - timedelta(days=7)]

                carried = not in_window
                current = in_window[-1] if in_window else last_print
                wow_pct = None
                if in_window and prior_window and prior_window[-1]['px']:
                    wow_pct = round((in_window[-1]['px'] - prior_window[-1]['px'])
                                    / prior_window[-1]['px'] * 100, 2)

                # 4-week / 52-week reference prints (actual prints only)
                def _last_on_or_before(d: date) -> Optional[float]:
                    pts = [p for p in hist if p['date'] <= d]
                    return pts[-1]['px'] if pts else None
                mom_ref = _last_on_or_before(close - timedelta(weeks=4))
                yoy_ref = _last_on_or_before(close - timedelta(weeks=52))
                mom_pct = (round((current['px'] - mom_ref) / mom_ref * 100, 2)
                           if (not carried and mom_ref) else None)
                yoy_pct = (round((current['px'] - yoy_ref) / yoy_ref * 100, 2)
                           if (not carried and yoy_ref) else None)

                lows = min(p['px'] for p in hist) * scale
                highs = max(p['px'] for p in hist) * scale

                cur.execute("""
                    INSERT INTO reports.feedstock_price_dashboard_snapshot
                        (issue_id, product, location, week_ending, feedstock_code,
                         weekly_avg, wow_change_pct, mom_change_pct, yoy_change_pct,
                         range_52w_low, range_52w_high, unit, source,
                         last_observed, is_carried_forward, is_placeholder)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE)
                    ON CONFLICT (issue_id, product, location) DO UPDATE SET
                        weekly_avg = EXCLUDED.weekly_avg,
                        wow_change_pct = EXCLUDED.wow_change_pct,
                        mom_change_pct = EXCLUDED.mom_change_pct,
                        yoy_change_pct = EXCLUDED.yoy_change_pct,
                        range_52w_low = EXCLUDED.range_52w_low,
                        range_52w_high = EXCLUDED.range_52w_high,
                        last_observed = EXCLUDED.last_observed,
                        is_carried_forward = EXCLUDED.is_carried_forward,
                        is_placeholder = FALSE,
                        source = EXCLUDED.source, unit = EXCLUDED.unit,
                        week_ending = EXCLUDED.week_ending
                """, (issue['id'], FEEDSTOCK_LABELS[code], spec['location_label'],
                      close, code,
                      round(current['px'] * scale, 2), wow_pct, mom_pct, yoy_pct,
                      round(lows, 2), round(highs, 2), spec['unit'],
                      spec['public_source'], current['date'], carried))
                n += 1
                age = (close - current['date']).days
                tag = ('CARRIED' if carried else 'in-window')
                if age > STALE_EXCLUDE_DAYS:
                    tag += f' — STALE >{STALE_EXCLUDE_DAYS}d, renderer will exclude'
                logger.info(f"{code} {spec['location_label']}: "
                            f"{current['px'] * scale:.2f} {spec['unit']} "
                            f"as of {current['date']} ({tag}), w/w={wow_pct}")
        conn.commit()
    return n


# =============================================================
# Credit stack snapshot
# =============================================================

def snapshot_credits(issue_no: int) -> int:
    """Insert per-instrument rows. As of the 2026-08-06 Task 0 audit there is
    NO whitelisted credit-price series in the database (bronze.credit_prices is
    Fastmarkets-sourced, barred from output; LCFS static since 2025-04-18), so
    collector-fed rows are pending — instruments land as coverage-pending rows
    unless/until `snapshot manual --kind credits` supplies whitelisted prints.
    """
    issue = get_issue(issue_no)
    n = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for inst in CREDIT_INSTRUMENTS:
                cur.execute("""
                    INSERT INTO reports.feedstock_credit_stack_snapshot
                        (issue_id, instrument, unit, notes)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (issue_id, instrument) WHERE instrument IS NOT NULL
                    DO UPDATE SET unit = EXCLUDED.unit
                """, (issue['id'], inst['instrument'], inst['unit'],
                      'collector pending — whitelisted source required '
                      '(manual path available)'))
                n += 1
        conn.commit()
    return n


# =============================================================
# Manual entry path (supervised CSV) — database only
# =============================================================

REQUIRED_PRICE_COLS = {'feedstock_code', 'price', 'unit', 'source', 'observation_date'}
REQUIRED_CREDIT_COLS = {'instrument', 'price', 'unit', 'source', 'observation_date'}
KNOWN_INSTRUMENTS = {i['instrument'] for i in CREDIT_INSTRUMENTS}


def load_manual_csv(issue_no: int, csv_path: str, kind: str = 'prices') -> int:
    """Load supervised manual entries through the same validation and snapshot
    tables as collector data. UCO rule (ruled 2026-08-06): CME Group settlement
    values only — any other source for UCO is rejected at load."""
    issue = get_issue(issue_no)
    close: date = issue['week_ending']
    required = REQUIRED_PRICE_COLS if kind == 'prices' else REQUIRED_CREDIT_COLS
    rows = list(csv.DictReader(open(csv_path, newline='', encoding='utf-8-sig')))
    if not rows:
        raise SystemExit(f"{csv_path}: empty CSV")
    missing = required - set(rows[0].keys())
    if missing:
        raise SystemExit(f"{csv_path}: missing required columns: {sorted(missing)}")

    n = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for i, r in enumerate(rows, 1):
                try:
                    price = float(r['price'])
                    obs = datetime.strptime(r['observation_date'].strip(), '%Y-%m-%d').date()
                except (ValueError, KeyError) as e:
                    raise SystemExit(f"{csv_path} row {i}: bad price/date ({e})")
                source = r['source'].strip()
                unit = r['unit'].strip()
                if not source:
                    raise SystemExit(f"{csv_path} row {i}: source is required")
                if obs > date.today():
                    raise SystemExit(f"{csv_path} row {i}: observation_date {obs} is in the future")

                if kind == 'prices':
                    code = r['feedstock_code'].strip().upper()
                    if code not in FEEDSTOCK_CODES:
                        raise SystemExit(f"{csv_path} row {i}: unknown feedstock_code "
                                         f"{code!r} (canonical: {FEEDSTOCK_CODES})")
                    if code == 'UCO' and source != 'CME Group':
                        raise SystemExit(
                            f"{csv_path} row {i}: UCO rows must be sourced 'CME Group' "
                            f"(settlement values only, ruled 2026-08-06); got {source!r}")
                    location = (r.get('location') or 'manual').strip()
                    cur.execute("""
                        INSERT INTO reports.feedstock_price_dashboard_snapshot
                            (issue_id, product, location, week_ending, feedstock_code,
                             weekly_avg, unit, source, last_observed,
                             is_carried_forward, is_manual_entry, is_placeholder)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,FALSE)
                        ON CONFLICT (issue_id, product, location) DO UPDATE SET
                            weekly_avg = EXCLUDED.weekly_avg,
                            unit = EXCLUDED.unit, source = EXCLUDED.source,
                            last_observed = EXCLUDED.last_observed,
                            is_carried_forward = EXCLUDED.is_carried_forward,
                            is_manual_entry = TRUE, is_placeholder = FALSE
                    """, (issue['id'], FEEDSTOCK_LABELS[code], location, close, code,
                          price, unit, source, obs,
                          not (close - timedelta(days=6) <= obs <= close)))
                else:
                    inst = r['instrument'].strip().upper()
                    if inst not in KNOWN_INSTRUMENTS:
                        raise SystemExit(f"{csv_path} row {i}: unknown instrument {inst!r} "
                                         f"(known: {sorted(KNOWN_INSTRUMENTS)})")
                    cur.execute("""
                        INSERT INTO reports.feedstock_credit_stack_snapshot
                            (issue_id, instrument, price, unit, source, last_observed,
                             is_carried_forward, is_manual_entry, notes)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,TRUE,NULL)
                        ON CONFLICT (issue_id, instrument) WHERE instrument IS NOT NULL
                        DO UPDATE SET
                            price = EXCLUDED.price, unit = EXCLUDED.unit,
                            source = EXCLUDED.source,
                            last_observed = EXCLUDED.last_observed,
                            is_carried_forward = EXCLUDED.is_carried_forward,
                            is_manual_entry = TRUE, notes = NULL
                    """, (issue['id'], inst, price, unit, source, obs,
                          not (close - timedelta(days=6) <= obs <= close)))
                n += 1
        conn.commit()
    logger.info(f"Loaded {n} manual {kind} rows into issue {issue_no}")
    return n


# =============================================================
# News + sections
# =============================================================

def add_news(issue_no: int, headline: str, url: str = '', source: str = '',
             note: str = '', rank: Optional[int] = None) -> None:
    issue = get_issue(issue_no)
    with get_connection() as conn:
        with conn.cursor() as cur:
            if rank is None:
                cur.execute("""SELECT COALESCE(MAX(sort_order), 0) + 1 AS r
                               FROM reports.feedstock_news_items WHERE issue_id = %s""",
                            (issue['id'],))
                rank = cur.fetchone()['r']
            cur.execute("""
                INSERT INTO reports.feedstock_news_items
                    (issue_id, headline, url, source, short_take, sort_order, published_at)
                VALUES (%s,%s,%s,%s,%s,%s,CURRENT_DATE)
            """, (issue['id'], headline, url, source, note, rank))
        conn.commit()


def set_section(issue_no: int, code: str, body_md: str, author: str = 'tore',
                title: Optional[str] = None) -> None:
    from src.reports.feedstock_report.report_config import SECTION_REGISTRY
    reg = {c: (o, t) for o, c, t, _k in SECTION_REGISTRY}
    if code not in reg:
        raise SystemExit(f"unknown section code {code!r} (registry: {sorted(reg)})")
    order, default_title = reg[code]
    issue = get_issue(issue_no)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reports.feedstock_section_content
                    (issue_id, section_code, section_number, title, prose,
                     word_count, author, last_edited_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (issue_id, section_code) DO UPDATE SET
                    prose = EXCLUDED.prose, title = EXCLUDED.title,
                    word_count = EXCLUDED.word_count, author = EXCLUDED.author,
                    last_edited_at = NOW()
            """, (issue['id'], code, order, title or default_title, body_md,
                  len(body_md.split()), author))
        conn.commit()


# =============================================================
# IFV leaderboard (computed; numbers stay in the DB snapshot —
# free-mode render is rank + direction arrows ONLY)
# =============================================================

def build_ifv_leaderboard(issue_no: int) -> List[Dict]:
    from src.kg.callables.implied_feedstock_value import run as ifv_run
    import json as _json
    issue = get_issue(issue_no)
    as_of = issue['week_ending']

    entries = []
    for code, ifv_code in IFV_LEADERBOARD_CODES.items():
        try:
            out = ifv_run(fuel='renewable_diesel', region='midwest',
                          feedstock_code=ifv_code, as_of_date=as_of.isoformat())
            if 'error' in out:
                logger.warning(f"IFV {code}: {out['error']}")
                continue
            entries.append({'code': code, 'label': FEEDSTOCK_LABELS[code],
                            'ifv_per_lb': out['implied_bid_per_lb']})
        except Exception as e:
            logger.warning(f"IFV {code} failed: {e}")

    entries.sort(key=lambda e: -e['ifv_per_lb'])
    # Direction arrows vs the PRIOR issue's stored leaderboard.
    prior_ranks: Dict[str, int] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT sc.data_snapshot
                FROM reports.feedstock_section_content sc
                JOIN reports.feedstock_issue i ON i.id = sc.issue_id
                WHERE sc.section_code = 'ifv_leaderboard' AND i.issue_number < %s
                ORDER BY i.issue_number DESC LIMIT 1
            """, (issue_no,))
            row = cur.fetchone()
            if row and row['data_snapshot']:
                for e in row['data_snapshot'].get('entries', []):
                    prior_ranks[e['code']] = e['rank']

    for i, e in enumerate(entries, 1):
        e['rank'] = i
        prev = prior_ranks.get(e['code'])
        e['direction'] = ('new' if prev is None else
                          'up' if i < prev else 'down' if i > prev else 'flat')

    snapshot = {'as_of': as_of.isoformat(), 'entries': entries}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reports.feedstock_section_content
                    (issue_id, section_code, section_number, title, data_snapshot, author)
                VALUES (%s, 'ifv_leaderboard', 5, 'IFV Leaderboard', %s, 'agent')
                ON CONFLICT (issue_id, section_code) DO UPDATE SET
                    data_snapshot = EXCLUDED.data_snapshot, last_edited_at = NOW()
            """, (issue['id'], _json.dumps(snapshot)))
        conn.commit()
    logger.info(f"IFV leaderboard: {[(e['rank'], e['code']) for e in entries]}")
    return entries
