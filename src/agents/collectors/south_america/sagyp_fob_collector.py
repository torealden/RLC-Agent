"""SAGyP official Argentine FOB prices -> bronze.sagyp_fob_raw + silver.price_mark.

Endpoint quirks (verified by live probe 2026-08-05, mig 173 has the full rationale):
  - Content-Type says text/html but the body is JSON {"posts":[...]} — parse the body.
  - Keys "añoDesde"/"añoHasta" carry a real unicode ñ.
  - "fecha" arrives as "1993-01-04 00:00:00.000".
  - Rows repeat per posicion with different mesDesde-mesHasta ranges: forward SHIPMENT BANDS,
    the core value of the series. Never collapse to one price per product.
  - Published ART business days only. An empty posts[] on a weekday is a probable Argentine
    holiday: INFO no-publication, SUCCESS with 0 rows — not a failure event.
  - History works back to 1993-01-04. Posicion codes are stable across the whole span.

Silver promotion maps ONLY curated exact posiciones (reference.sagyp_position_map) to
price_mark WINDOW rows, tenor 'YYYY-MM:YYYY-MM' from the band. Band boundaries are data.

Scheduler-facing contract (registered 'sagyp_fob_oficial', daily 18:00 ET): collect() pulls
today (ART) plus the previous business day (late postings / same-day revisions — the circular
number changes on revision; the upsert absorbs both), returns CollectorResult, no self-logging.
"""
from __future__ import annotations

import json
import logging
import sys
import time as _time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.services.database.db_config import get_connection

logger = logging.getLogger(__name__)

ART = ZoneInfo("America/Argentina/Buenos_Aires")
PRIMARY_BASE = "https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/ws/ssma/precios_fob.php"
FALLBACK_BASE = "https://monitorsiogranos.magyp.gob.ar/ws/ssma/precios_fob.php"  # original host
USER_AGENT = "RLC-Agent/1.0 (Round Lakes Commodities; tore.alden@roundlakescommodities.com)"

UPSERT_BRONZE = """
    INSERT INTO bronze.sagyp_fob_raw
        (fecha, circular, posicion, precio, mes_desde, anio_desde, mes_hasta, anio_hasta)
    VALUES %s
    ON CONFLICT (fecha, posicion, mes_desde, anio_desde) DO UPDATE SET
        circular = EXCLUDED.circular,
        precio = EXCLUDED.precio,
        mes_hasta = EXCLUDED.mes_hasta,
        anio_hasta = EXCLUDED.anio_hasta,
        collected_at = now()
"""

# Set-based promotion so backfill and daily use one code path. make_date guards
# against a malformed month; rows that fail curation just stay bronze-only.
PROMOTE_SILVER = """
    INSERT INTO silver.price_mark
        (series_key, obs_date, tenor_type, tenor, value, unit, currency,
         source, quality_rank, can_republish)
    SELECT m.series_key, b.fecha, 'WINDOW',
           to_char(make_date(b.anio_desde, b.mes_desde, 1), 'YYYY-MM')
               || ':' || to_char(make_date(b.anio_hasta, b.mes_hasta, 1), 'YYYY-MM'),
           b.precio, 'USD/t', 'USD', 'sagyp_oficial', 'OFFICIAL_GOV', true
    FROM bronze.sagyp_fob_raw b
    JOIN reference.sagyp_position_map m ON m.posicion = b.posicion AND m.is_active
    WHERE b.fecha BETWEEN %s AND %s
      AND b.precio IS NOT NULL
      AND b.mes_desde BETWEEN 1 AND 12 AND b.mes_hasta BETWEEN 1 AND 12
    ON CONFLICT (series_key, obs_date, tenor_type, tenor, source) DO UPDATE SET
        value = EXCLUDED.value,
        collected_at = now()
"""


def fetch_day(fecha: date, session: Optional[requests.Session] = None,
              timeout: int = 60) -> List[Dict]:
    """Fetch one publication date. Returns the raw posts list ([] = no publication)."""
    ses = session or requests.Session()
    params = {"Fecha": fecha.strftime("%d/%m/%Y")}
    last_err: Optional[Exception] = None
    for base in (PRIMARY_BASE, FALLBACK_BASE):
        for attempt in range(3):
            try:
                r = ses.get(base, params=params, timeout=timeout,
                            headers={"User-Agent": USER_AGENT})
                r.raise_for_status()
                # Content-Type lies (text/html); trust the body. Publication days
                # return {"posts":[...]}; no-publication days return a bare [].
                body = json.loads(r.text)
                if isinstance(body, dict):
                    return body.get("posts", []) or []
                return body or []
            except (requests.RequestException, json.JSONDecodeError) as e:
                last_err = e
                _time.sleep(2 ** attempt)
    raise RuntimeError(f"SAGyP fetch failed for {fecha} on both hosts: {last_err}")


def parse_posts(fecha: date, posts: List[Dict]) -> List[tuple]:
    """Shape raw posts into bronze tuples. Handles the unicode-ñ keys explicitly."""
    rows = []
    for p in posts:
        try:
            precio = p.get("precio")
            precio = float(precio) if precio is not None else None
        except (TypeError, ValueError):
            precio = None  # 's/c' (sin cotización) and friends land as NULL price
        try:
            rows.append((
                fecha,
                str(p.get("circular")) if p.get("circular") is not None else None,
                str(p["posicion"]),
                precio,
                int(p["mesDesde"]), int(p["añoDesde"]),
                int(p["mesHasta"]) if p.get("mesHasta") is not None else None,
                int(p["añoHasta"]) if p.get("añoHasta") is not None else None,
            ))
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"sagyp_fob: unparseable row skipped ({e}): {p}")
    return rows


def _circ_num(circular) -> int:
    try:
        return int(circular)
    except (TypeError, ValueError):
        return -1


def dedupe(rows: List[tuple]) -> List[tuple]:
    """One day's response can carry the SAME band twice — original + revision circular
    both present (observed in the 2020s backfill). Keep the highest circular number;
    on a tie the later row wins. Required: duplicate PKs in one INSERT raise
    CardinalityViolation before ON CONFLICT can arbitrate."""
    best: Dict[tuple, tuple] = {}
    for r in rows:
        k = (r[0], r[2], r[4], r[5])  # fecha, posicion, mes_desde, anio_desde
        prev = best.get(k)
        if prev is None or _circ_num(r[1]) >= _circ_num(prev[1]):
            best[k] = r
    return list(best.values())


def persist(rows_by_date: Dict[date, List[tuple]]) -> int:
    """Upsert bronze + promote curated silver for the touched dates. Returns bronze rows."""
    from psycopg2.extras import execute_values
    all_rows = dedupe([r for rows in rows_by_date.values() for r in rows])
    dates = sorted(rows_by_date.keys())
    with get_connection() as conn:
        cur = conn.cursor()
        if all_rows:
            execute_values(cur, UPSERT_BRONZE, all_rows, page_size=1000)
        if dates:
            cur.execute(PROMOTE_SILVER, (dates[0], dates[-1]))
        conn.commit()
    return len(all_rows)


# Unmapped-position monitor (mig 175): a posicion sharing a curated HS6 family with NO
# disposition row is a new/drifted code — surface it instead of letting the series die
# silently in bronze. Known-but-unpromoted variants have series_key NULL rows and don't fire.
UNREVIEWED_POSITIONS = """
    SELECT DISTINCT b.posicion
    FROM bronze.sagyp_fob_raw b
    WHERE b.fecha BETWEEN %s AND %s
      AND left(b.posicion, 6) IN (SELECT left(posicion, 6)
                                  FROM reference.sagyp_position_map WHERE is_active)
      AND NOT EXISTS (SELECT 1 FROM reference.sagyp_position_map m
                      WHERE m.posicion = b.posicion)
"""


def unreviewed_positions(dates: List[date]) -> List[str]:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(UNREVIEWED_POSITIONS, (min(dates), max(dates)))
        return sorted(r["posicion"] for r in cur.fetchall())


def prev_business_day(d: date) -> date:
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


class SAGyPFOBCollector:
    """Scheduler-facing wrapper (EIAV2CrudeDaily pattern): CollectorResult contract,
    no self-logging, no ET-naive timestamps."""

    COLLECTOR_NAME = "sagyp_fob_oficial"

    def collect(self, dates: Optional[List[date]] = None, **kwargs):
        from datetime import datetime
        from src.agents.base.base_collector import CollectorResult

        if dates is None:
            today_art = datetime.now(ART).date()
            dates = [prev_business_day(today_art), today_art]
            # Weekend fire (shouldn't happen — dispatcher runs mon-fri): just cover
            # the two most recent business days instead.
            if today_art.weekday() >= 5:
                d1 = prev_business_day(today_art)
                dates = [prev_business_day(d1), d1]

        try:
            session = requests.Session()
            rows_by_date: Dict[date, List[tuple]] = {}
            empty_days: List[date] = []
            for d in dates:
                posts = fetch_day(d, session)
                if not posts:
                    empty_days.append(d)
                    logger.info(f"sagyp_fob: no publication for {d} "
                                f"(probable Argentine holiday) — not a failure")
                    continue
                rows_by_date[d] = parse_posts(d, posts)
            n = persist(rows_by_date) if rows_by_date else 0
            new_codes = unreviewed_positions(list(rows_by_date)) if rows_by_date else []
        except Exception as e:
            logger.error(f"sagyp_fob_oficial failed: {e}")
            return CollectorResult(success=False, source=self.COLLECTOR_NAME,
                                   error_message=str(e))

        warnings = []
        if new_codes:
            warnings.append(
                f"unreviewed posicion(s) in curated HS6 families: {new_codes} — add "
                f"disposition rows to reference.sagyp_position_map (map or mark bronze-only)")
        return CollectorResult(
            success=True,  # empty weekday = holiday no-publication, still SUCCESS
            source=self.COLLECTOR_NAME,
            records_fetched=n,
            period_start=min(dates).isoformat(),
            period_end=max(dates).isoformat(),
            warnings=warnings,
        )


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="dd/mm/yyyy or yyyy-mm-dd; default today ART + prev biz day")
    args = ap.parse_args()
    dates = None
    if args.date:
        s = args.date
        d = (date(*map(int, s.split("-"))) if "-" in s
             else date(int(s[6:10]), int(s[3:5]), int(s[0:2])))
        dates = [d]
    res = SAGyPFOBCollector().collect(dates=dates)
    print(res.success, res.records_fetched, res.error_message)
