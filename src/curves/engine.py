"""Curve engine: assemble derived-curve term stacks and write them transactionally.

For each spec in specs.CURVE_SPECS and each observation date in a trailing window, the engine:

  1. reads the parent's DATED contract marks from silver.price_mark, best quality_rank per tenor
     (so official settles automatically displace delayed data when both exist), never NEARBY/FRONT
     (brief §D.2 aggregator rule) and never PLACEHOLDER-ranked rows (§D.1);
  2. refuses tenors whose reported volume is below the spec threshold (thin guard; OI is NULL in
     the delayed source, so volume is the §D.1 proxy);
  3. maps each contract code to a delivery-window tenor (ZL_U26 -> '2026-09');
  4. builds the term stack (board + spec fixed terms) and writes, in ONE transaction per
     (curve, obs_date): DELETE the engine's previous terms + headline, INSERT term rows into
     gold.curve_term, INSERT the DERIVED_* headline into silver.price_mark with
     series_key == curve_key. The deferred curve_term_tieout trigger then proves
     SUM(term_value) == headline at COMMIT — the engine never bypasses or pre-empts it.

The headline is stored to price_mark rounded; term rows carry full precision. The trigger's
epsilon GREATEST(0.01, |headline|*1e-4) absorbs the rounding, and nothing else — a real
decomposition error still fails loud.
"""

from __future__ import annotations

import logging
from datetime import datetime

from src.curves.specs import CURVE_SPECS, ParityChainSpec

logger = logging.getLogger(__name__)

_MONTH_CODES = {"F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
                "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12}

ENGINE_SOURCE = "rlc_curve_engine"


def contract_tenor_to_window(tenor: str) -> str | None:
    """'ZL_U26' -> '2026-09'. Returns None for codes that don't parse (logged, skipped)."""
    code = tenor.rsplit("_", 1)[-1]
    if len(code) != 3 or code[0] not in _MONTH_CODES or not code[1:].isdigit():
        return None
    month = _MONTH_CODES[code[0]]
    yy = int(code[1:])
    year = 1900 + yy if yy >= 70 else 2000 + yy
    return f"{year:04d}-{month:02d}"


class CurveEngine:
    COLLECTOR_NAME = "curve_builder"

    def __init__(self, specs=None, window_dates: int = 10):
        self.specs = specs if specs is not None else CURVE_SPECS
        self.window_dates = window_dates  # trailing obs_dates re-derived each run (revision heal)

    def collect(self, triggered_by: str = "manual") -> dict:
        started = datetime.now()
        try:
            results = self.build_all()
        except Exception as e:
            logger.error("curve build failed: %s", e)
            self._log_run(started, "FAILED", 0, error=str(e), triggered_by=triggered_by)
            return {"success": False, "error": str(e)}
        total_terms = sum(r["term_rows"] for r in results)
        latest = max((r["obs_date"] for r in results), default=None)
        self._log_run(started, "SUCCESS", total_terms,
                      data_period=str(latest) if latest else None, triggered_by=triggered_by)
        return {"success": True, "curves": len(self.specs), "builds": len(results),
                "term_rows": total_terms, "latest": str(latest) if latest else None}

    def build_all(self) -> list[dict]:
        from src.services.database.db_config import get_connection
        results = []
        with get_connection() as conn:
            for spec in self.specs:
                for obs_date in self._parent_dates(conn, spec):
                    res = self.build_curve(conn, spec, obs_date)
                    if res:
                        results.append(res)
        return results

    def _parent_dates(self, conn, spec: ParityChainSpec):
        cur = conn.cursor()
        cur.execute(
            """SELECT DISTINCT obs_date FROM silver.price_mark
               WHERE series_key = %s AND tenor_type = 'CONTRACT'
               ORDER BY obs_date DESC LIMIT %s""",
            (spec.parent_series, self.window_dates),
        )
        rows = cur.fetchall()
        return sorted(r["obs_date"] if isinstance(r, dict) else r[0] for r in rows)

    def build_curve(self, conn, spec: ParityChainSpec, obs_date) -> dict | None:
        """Build one (curve, obs_date) stack in one transaction. Returns build stats or None."""
        cur = conn.cursor()
        # Board marks come from the ruled consumer path gold.price_mark_best (mig 160): one row
        # per cell, official > delayed by rank ordinal then source tier, PLACEHOLDER excluded.
        cur.execute(
            """SELECT pm.tenor, pm.value, pm.source, pm.quality_rank,
                      (SELECT MAX(cs.volume) FROM silver.curve_snapshot cs
                       WHERE cs.series_key = pm.series_key AND cs.obs_date = pm.obs_date
                         AND cs.contract = pm.tenor) AS volume
               FROM gold.price_mark_best pm
               WHERE pm.series_key = %s AND pm.obs_date = %s
                 AND pm.tenor_type = 'CONTRACT'
               ORDER BY pm.tenor""",
            (spec.parent_series, obs_date),
        )
        marks = cur.fetchall()
        if not marks:
            return None

        stacks = []  # (window_tenor, [(term_name, value, source, rank, note)])
        for m in marks:
            tenor, value, source, rank, volume = (
                (m["tenor"], m["value"], m["source"], m["quality_rank"], m["volume"])
                if isinstance(m, dict) else m)
            window = contract_tenor_to_window(tenor)
            if window is None:
                logger.warning("%s: unparseable contract tenor %s — skipped", spec.curve_key, tenor)
                continue
            if volume is None or volume < spec.min_volume:
                logger.info("%s %s %s: volume %s below %s — tenor refused (thin guard)",
                            spec.curve_key, obs_date, tenor, volume, spec.min_volume)
                continue
            terms = [(
                "board", float(value) * spec.unit_factor, f"{source} {tenor}", rank,
                spec.unit_note,
            )]
            terms += [(t.name, t.value, t.source, t.quality_rank, t.method_note)
                      for t in spec.fixed_terms]
            stacks.append((window, terms))

        if not stacks:
            return None

        # One transaction: replace the engine's previous stack for this (curve, obs_date),
        # then let the deferred tie-out validate every touched group at COMMIT.
        try:
            cur.execute("DELETE FROM gold.curve_term WHERE curve_key = %s AND obs_date = %s",
                        (spec.curve_key, obs_date))
            cur.execute(
                """DELETE FROM silver.price_mark
                   WHERE series_key = %s AND obs_date = %s AND source = %s""",
                (spec.curve_key, obs_date, ENGINE_SOURCE))
            term_rows = 0
            for window, terms in stacks:
                headline = sum(v for _, v, _, _, _ in terms)
                for name, value, source, rank, note in terms:
                    cur.execute(
                        """INSERT INTO gold.curve_term
                           (curve_key, obs_date, tenor, term_name, term_value,
                            term_source, quality_rank, method_note)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (spec.curve_key, obs_date, window, name, value, source, rank, note))
                    term_rows += 1
                cur.execute(
                    """INSERT INTO silver.price_mark
                       (series_key, obs_date, tenor_type, tenor, value, unit, currency,
                        source, quality_rank, can_republish)
                       VALUES (%s,%s,'WINDOW',%s,%s,%s,%s,%s,%s,%s)""",
                    (spec.curve_key, obs_date, window, round(headline, 4), spec.unit,
                     spec.currency, ENGINE_SOURCE, spec.quality_rank, spec.can_republish))
            conn.commit()  # deferred curve_term_tieout fires HERE
        except Exception:
            conn.rollback()
            raise
        return {"curve_key": spec.curve_key, "obs_date": obs_date,
                "tenors": len(stacks), "term_rows": term_rows}

    def _log_run(self, started, status, rows, data_period=None, error=None, triggered_by="manual"):
        try:
            from src.services.database.db_config import get_connection
            with get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """INSERT INTO core.collection_status
                       (collector_name, run_started_at, run_finished_at, status, rows_collected,
                        rows_inserted, error_message, data_period, commodities, is_new_data, triggered_by)
                       VALUES (%s,%s,now(),%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (self.COLLECTOR_NAME, started, status, rows, rows, error, data_period,
                     ["curves"], rows > 0, triggered_by),
                )
                conn.commit()
        except Exception as e:  # pragma: no cover
            logger.error("collection_status log failed: %s", e)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(CurveEngine().collect(triggered_by="cli"))


if __name__ == "__main__":
    main()
