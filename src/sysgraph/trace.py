"""Graph traversal -- the thing Q1 actually needs.

`trace(key, 'down')` walks with the arrows (design 5.2: edges point the way data flows), so
downstream of a series is every gold view, script, VBA procedure, workbook and deliverable
that ends up carrying it. `'up'` walks against them, back toward the external source.

Two rules from D4/R4 are enforced here rather than left to the caller:

  * `resolution_status <> 'resolved'` hops are excluded by default. An edge that points at
    nothing is not lineage.
  * `lifecycle` NEVER excludes. A superseded workbook that is still wired in is exactly the
    finding this graph exists to surface -- the live `eia_data.xlsm` chain would have been
    buried by a lifecycle filter. Superseded hops come back labelled, not hidden.
"""

from __future__ import annotations

# Two kinds of edge, and conflating them is the difference between a useful blast radius and
# a useless one.
#
#   FLOW edges (READS, WRITES, DERIVES_FROM, EMITS, LINKS_TO, BINDS_TO, ...) are directional.
#   A `down` walk follows them; an `up` walk reverses them.
#
#   CONTAINMENT edges (HAS_SERIES, DEFINES) say "this is part of that". They are stored
#   container -> member because that is how a person reads them, but they are only ever
#   TRAVERSED member -> container, in BOTH walk directions. That single rule is what makes
#   Q1 work: from `monthly_realized[attribute=oil_stocks]` you reach `silver.monthly_realized`
#   and therefore everything that consumes it -- while never reaching the 228 sibling series
#   in the same table, which have nothing to do with the oil_stocks defect.
CONTAINMENT = ("HAS_SERIES", "DEFINES")

WALK_SQL = """
WITH RECURSIVE seed AS (
    SELECT node_id FROM sys.node WHERE node_key = %(key)s
),
walk AS (
    SELECT n.node_id, n.node_key, n.node_type, n.lifecycle,
           0 AS depth,
           NULL::text AS via_edge,
           1.00::numeric AS path_confidence,
           ARRAY[n.node_id] AS visited
      FROM sys.node n JOIN seed s ON s.node_id = n.node_id

    UNION ALL

    SELECT nx.node_id, nx.node_key, nx.node_type, nx.lifecycle,
           w.depth + 1,
           e.edge_type,
           LEAST(w.path_confidence, e.confidence),
           w.visited || nx.node_id
      FROM walk w
      JOIN sys.edge e
        ON (e.edge_type <> ALL(%(containment)s) AND e.{from_col} = w.node_id)
        OR (e.edge_type  = ANY(%(containment)s) AND e.target_node_id = w.node_id)
      JOIN sys.node nx
        ON nx.node_id = CASE WHEN e.edge_type = ANY(%(containment)s)
                             THEN e.source_node_id ELSE e.{to_col} END
     WHERE w.depth < %(max_depth)s
       AND NOT nx.node_id = ANY(w.visited)
       AND (%(include_unresolved)s OR e.resolution_status = 'resolved')
       AND e.confidence >= %(min_confidence)s
       AND e.last_seen_scan = (SELECT scan_id FROM sys.v_current_scan)
)
SELECT DISTINCT ON (node_key)
       node_key, node_type, lifecycle, depth, via_edge, path_confidence
  FROM walk
 WHERE depth > 0
 ORDER BY node_key, depth
"""


def trace(conn, node_key: str, direction: str = "down", max_depth: int = 8,
          include_unresolved: bool = False, min_confidence: float = 0.50) -> list[dict]:
    """min_confidence defaults to 0.50, which excludes the `intent='mention'` edges.

    A bare `gold.x` in a docstring or a log line is a 0.40 edge. There are ~4,600 of them and
    traversing them turns a blast radius into a list of everything. They stay in the graph and
    stay queryable at min_confidence=0.0 -- they just do not get to claim they are lineage.
    """
    if direction not in ("down", "up"):
        raise ValueError("direction must be 'down' or 'up'")
    from_col, to_col = ("source_node_id", "target_node_id") if direction == "down" \
        else ("target_node_id", "source_node_id")
    cur = conn.cursor()
    cur.execute(
        WALK_SQL.format(from_col=from_col, to_col=to_col),
        {"key": node_key, "max_depth": max_depth, "include_unresolved": include_unresolved,
         "containment": list(CONTAINMENT), "min_confidence": min_confidence},
    )
    return [dict(r) for r in cur.fetchall()]


def trace_series(conn, series_key: str, direction: str = "down", **kw) -> list[dict]:
    """Convenience wrapper. If the exact series key is not a node, fall back to every series
    node whose key matches the given prefix/fragment -- `attribute=oil_stocks` should work
    without the caller reciting the full commodity/source tuple."""
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sys.node WHERE node_key = %s", (series_key,))
    if cur.fetchone():
        return trace(conn, series_key, direction, **kw)

    cur.execute(
        "SELECT node_key FROM sys.v_node WHERE node_type = 'data_series' AND node_key LIKE %s",
        (f"%{series_key}%",),
    )
    keys = [r["node_key"] for r in cur.fetchall()]
    seen, out = set(), []
    for k in keys:
        for hop in trace(conn, k, direction, **kw):
            if hop["node_key"] in seen:
                continue
            seen.add(hop["node_key"])
            hop["from_series"] = k
            out.append(hop)
    return out
