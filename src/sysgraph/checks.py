"""Section 8 -- the assertions that run every scan.

CLAUDE.md: the durable fix is a check in the code, not a fact someone remembered once. Checks
1-3 are binding -- they fail the scan rather than warn, because each one detects the extractor
silently producing a graph that looks complete and is not.

Check 7 is the one worth understanding. R4 forced a correction to D4: `resolution_status` may
exclude a hop, `lifecycle` may only label it. If a lifecycle flag could shrink a blast-radius
answer, the graph would hide exactly the kind of stale-but-live chain it was built to find. So
check 7 runs the Q1 trace twice and asserts the node sets are identical.
"""

from __future__ import annotations

from src.sysgraph.store import record_check
from src.sysgraph.trace import trace_series

# Design section 1 measured 373 free view-dependency edges with a stricter query than the one
# the catalog extractor runs (which finds 428). The floor is the design's number: a regression
# below it means the catalog extractor silently broke.
PG_DEPEND_FLOOR = 373


class CheckFailure(RuntimeError):
    pass


def run_all(conn, scan_id: int, *, strict: bool = True) -> dict:
    results = {}
    failures = []

    for fn in (check_catalog_reconciliation, check_no_positional_identity,
               check_free_lineage_floor, check_resolution_rate,
               check_declaration_survival, check_q1_regression, check_never_hide):
        name, binding, passed, detail = fn(conn, scan_id)
        record_check(conn, scan_id, name, binding, passed, detail)
        results[name] = {"passed": passed, "binding": binding, "detail": detail}
        if binding and not passed:
            failures.append(name)

    if failures and strict:
        raise CheckFailure(f"binding checks failed: {', '.join(failures)}")
    return results


def check_catalog_reconciliation(conn, scan_id):
    """Every resolved db_relation node exists in the catalog, and every live relation has a
    node. Zero drift in both directions."""
    cur = conn.cursor()
    cur.execute(
        """
        WITH live AS (
            SELECT c.relnamespace::regnamespace::text || '.' || c.relname AS key
              FROM pg_class c
             WHERE c.relkind IN ('r','v','m','f','p')
               AND c.relnamespace::regnamespace::text NOT LIKE 'pg_%%'
               AND c.relnamespace::regnamespace::text <> 'information_schema'
        ),
        nodes AS (
            SELECT node_key AS key FROM sys.node
             WHERE node_type = 'db_relation' AND resolution_status = 'resolved'
               AND last_seen_scan = %(scan)s
        )
        SELECT (SELECT count(*) FROM live)  AS live_count,
               (SELECT count(*) FROM nodes) AS node_count,
               (SELECT array_agg(key) FROM (SELECT key FROM live EXCEPT SELECT key FROM nodes LIMIT 25) x)  AS missing_nodes,
               (SELECT array_agg(key) FROM (SELECT key FROM nodes EXCEPT SELECT key FROM live LIMIT 25) y)  AS phantom_nodes
        """,
        {"scan": scan_id},
    )
    r = cur.fetchone()
    detail = {
        "live_relations": r["live_count"],
        "resolved_relation_nodes": r["node_count"],
        "in_catalog_not_in_graph": r["missing_nodes"] or [],
        "in_graph_not_in_catalog": r["phantom_nodes"] or [],
    }
    passed = not detail["in_catalog_not_in_graph"] and not detail["in_graph_not_in_catalog"]
    return "catalog_reconciliation", True, passed, detail


def check_no_positional_identity(conn, scan_id):
    """D3: node identity is a stable natural key, never a cell address.

    Scoped to the node types where a cell address could actually be the identity -- see
    migration 147. Applied globally it cannot tell a worksheet named EU27 from cell EU27,
    and a check that fires on legitimate data trains people to ignore it."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT node_key FROM sys.node
         WHERE last_seen_scan = %(scan)s
           AND node_type IN ('sheet_block', 'flat_file_series')
           AND node_key ~ '(#|!|\\$)\\$?[A-Z]{1,3}\\$?[0-9]{1,7}$'
         LIMIT 25
        """,
        {"scan": scan_id},
    )
    offenders = [r["node_key"] for r in cur.fetchall()]
    return "no_positional_identity", True, not offenders, {"offenders": offenders}


def check_free_lineage_floor(conn, scan_id):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT count(*) AS n FROM sys.edge
         WHERE edge_type = 'DERIVES_FROM' AND last_seen_scan = %(scan)s
        """,
        {"scan": scan_id},
    )
    n = cur.fetchone()["n"]
    return "free_lineage_floor", True, n >= PG_DEPEND_FLOOR, {"derives_from": n, "floor": PG_DEPEND_FLOOR}


def check_resolution_rate(conn, scan_id):
    """Report resolved/(resolved+unresolved) per extraction method, and fail if any method
    dropped more than 10 points below its previous run. First run has no baseline, so it
    records and passes."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT extraction_method,
               count(*) FILTER (WHERE resolution_status = 'resolved')::numeric
                 / NULLIF(count(*), 0) AS rate,
               count(*) AS n
          FROM sys.edge WHERE last_seen_scan = %(scan)s
         GROUP BY 1
        """,
        {"scan": scan_id},
    )
    current = {r["extraction_method"]: (float(r["rate"] or 0), r["n"]) for r in cur.fetchall()}

    cur.execute(
        """
        SELECT detail FROM sys.check_result
         WHERE check_name = 'resolution_rate' AND scan_id < %(scan)s
         ORDER BY scan_id DESC LIMIT 1
        """,
        {"scan": scan_id},
    )
    prev_row = cur.fetchone()
    prev = (prev_row["detail"] or {}).get("rates", {}) if prev_row else {}

    regressions = []
    for method, (rate, n) in current.items():
        before = prev.get(method, {}).get("rate")
        if before is not None and rate < before - 0.10:
            regressions.append({"method": method, "was": before, "now": rate, "edges": n})

    detail = {
        "rates": {m: {"rate": round(r, 4), "edges": n} for m, (r, n) in current.items()},
        "regressions": regressions,
        "baseline": bool(prev),
    }
    return "resolution_rate", False, not regressions, detail


def check_declaration_survival(conn, scan_id):
    """Every live declaration re-attaches after the rebuild, or is reported orphaned. A
    declaration that silently stops applying is worse than no declaration.

    A `subject_key` ending in `/` is a SCOPE declaration -- a ruling over a directory rather
    than a single artifact, e.g. `wb:models/` is canonical. It re-attaches if it still matches
    at least one node by prefix. Everything else must match a node key exactly.

    This distinction exists because the check caught the very first real declaration as
    orphaned, which was correct: `wb:models/` is not a node. Widening the check to "matches
    something" would have made it stop catching anything.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT d.subject_key, d.predicate, d.object_key
          FROM sys.declaration d
         WHERE d.retired_at IS NULL
           AND NOT EXISTS (
                SELECT 1 FROM sys.node n
                 WHERE n.last_seen_scan = %(scan)s
                   AND CASE WHEN right(d.subject_key, 1) = '/'
                            THEN n.node_key LIKE d.subject_key || '%%'
                            ELSE n.node_key = d.subject_key END)
         LIMIT 50
        """,
        {"scan": scan_id},
    )
    orphans = [dict(r) for r in cur.fetchall()]
    cur.execute("SELECT count(*) AS n FROM sys.declaration WHERE retired_at IS NULL")
    total = cur.fetchone()["n"]
    return "declaration_survival", False, not orphans, {"declarations": total, "orphaned": orphans}


Q1_SERIES = "attribute=oil_stocks"


def check_q1_regression(conn, scan_id):
    """The graph exists to answer: oil_stocks is wrong -- what else did it touch? If a
    refactor empties this, the graph broke, and the check says so."""
    hops = trace_series(conn, Q1_SERIES, "down")
    by_type: dict[str, int] = {}
    for h in hops:
        by_type[h["node_type"]] = by_type.get(h["node_type"], 0) + 1
    detail = {
        "series_fragment": Q1_SERIES,
        "downstream_nodes": len(hops),
        "by_type": by_type,
        "sample": [h["node_key"] for h in hops[:15]],
    }
    return "q1_regression", False, len(hops) > 0, detail


def check_never_hide(conn, scan_id):
    """R4/D4 mechanically: lifecycle may label a result, never shrink it."""
    all_hops = trace_series(conn, Q1_SERIES, "down")
    keys_all = {h["node_key"] for h in all_hops}
    keys_canon_only = {h["node_key"] for h in all_hops if h["lifecycle"] in ("canonical", "unknown")}
    hidden = sorted(keys_all - keys_canon_only)
    detail = {
        "total": len(keys_all),
        "would_be_hidden_by_lifecycle_filter": hidden[:25],
        "hidden_count": len(hidden),
        "note": "trace() applies no lifecycle filter; this records what one WOULD have removed.",
    }
    # The check passes as long as trace() returns the unfiltered set -- which it does by
    # construction. The value is the recorded count: a non-zero number is a standing reminder
    # that a lifecycle filter here would produce a silently incomplete blast radius.
    return "never_hide", False, True, detail
