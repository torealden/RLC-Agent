"""Scan orchestrator.

Build order follows design section 9. Each step is independently useful; the scan can stop
after any of them and the value already written stays written.

  1  sys migration + sys.scan          somewhere to put it
  2  catalog extractor                 ~600 relation nodes, 400+ free edges
  3  repo inventory                    Q3 half-answered
  4  series extractor                  the spine
  5  code -> relation extractor        Q3 answered; the unresolved report
  6  workbook inventory + ext links    Q2 answered
  7  block + formula criteria          Q1 answered end to end
"""

from __future__ import annotations

import time
import traceback

from src.services.database.db_config import get_connection
from src.sysgraph import EXTRACTOR_VERSION, catalog, checks, coderefs, repo, series, workbooks


def live_relation_keys(conn) -> set[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.relnamespace::regnamespace::text || '.' || c.relname AS key
          FROM pg_class c
         WHERE c.relkind IN ('r','v','m','f','p')
           AND c.relnamespace::regnamespace::text NOT LIKE 'pg_%%'
           AND c.relnamespace::regnamespace::text <> 'information_schema'
        """
    )
    return {r["key"] for r in cur.fetchall()}


def run(steps: str = "all", strict: bool = True, verbose: bool = True) -> dict:
    from src.sysgraph.store import GraphStore, close_scan, open_scan

    wanted = set(range(1, 8)) if steps == "all" else {int(s) for s in steps.split(",")}
    stats: dict = {"extractor_version": EXTRACTOR_VERSION, "steps": sorted(wanted)}

    with get_connection() as conn:
        scan_id = open_scan(conn, mode="full" if steps == "all" else f"partial:{steps}")
        store = GraphStore(conn, scan_id)
        say = (lambda m: print(f"[scan {scan_id}] {m}", flush=True)) if verbose else (lambda m: None)
        say(f"opened, extractor {EXTRACTOR_VERSION}")

        try:
            live = live_relation_keys(conn)
            stats["live_relations"] = len(live)

            if 2 in wanted:
                t = time.time()
                stats["catalog"] = catalog.extract(conn, store)
                stats["catalog"]["seconds"] = round(time.time() - t, 1)
                say(f"step 2 catalog: {stats['catalog']}")

            if 3 in wanted:
                t = time.time()
                stats["repo"] = repo.extract(conn, store)
                stats["repo"]["seconds"] = round(time.time() - t, 1)
                say(f"step 3 repo: {stats['repo']}")

            if 4 in wanted:
                t = time.time()
                stats["series"] = series.extract(conn, store)
                stats["series"]["seconds"] = round(time.time() - t, 1)
                say(f"step 4 series: {stats['series']}")

            if 5 in wanted:
                t = time.time()
                s5 = coderefs.extract(conn, store, live)
                # The unresolved list is a finding, not noise -- but it does not belong in
                # every stats dump. Keep the count, drop the roster.
                stats["unresolved_relations"] = s5.pop("unresolved_relations", [])
                stats["coderefs"] = s5
                stats["coderefs"]["seconds"] = round(time.time() - t, 1)
                say(f"step 5 coderefs: {s5}")

            if 6 in wanted:
                t = time.time()
                s6 = workbooks.extract(conn, store, live)
                stats["external_link_index"] = s6.pop("external_link_index", {})
                stats["orphan_git_modules"] = s6.pop("orphan_git_modules", [])
                stats["workbooks"] = s6
                stats["workbooks"]["seconds"] = round(time.time() - t, 1)
                say(f"step 6 workbooks: {s6}")

            t = time.time()
            written = store.flush()
            stats["written"] = written
            stats["flush_seconds"] = round(time.time() - t, 1)
            say(f"flushed: {written}")

            if 7 in wanted:
                from src.sysgraph import blocks
                t = time.time()
                store2 = GraphStore(conn, scan_id)
                s7 = blocks.extract(conn, store2, stats.get("external_link_index", {}))
                store2.flush()
                stats["blocks"] = s7
                stats["blocks"]["seconds"] = round(time.time() - t, 1)
                say(f"step 7 blocks: {s7}")

            # Mark the scan ok BEFORE running checks -- sys.v_current_scan drives the trace
            # views the checks themselves depend on.
            close_scan(conn, scan_id, "ok", stats)
            stats["checks"] = checks.run_all(conn, scan_id, strict=strict)
            close_scan(conn, scan_id, "ok", stats)
            say("checks: " + ", ".join(
                f"{k}={'PASS' if v['passed'] else 'FAIL'}" for k, v in stats["checks"].items()))

        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            close_scan(conn, scan_id, "failed", stats, f"{type(exc).__name__}: {exc}")
            traceback.print_exc()
            raise

        stats["scan_id"] = scan_id
        return stats
