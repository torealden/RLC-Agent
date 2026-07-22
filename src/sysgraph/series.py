"""Step 4 -- the series extractor. This is the spine (design D2).

The `oil_stocks` defect is not a table and not a column: it is
`silver.monthly_realized WHERE attribute='oil_stocks'`. Column-level lineage says "everything
reads realized_value", which is true and useless. So the atom here is a relation plus a
resolvable key selector, and that grain is not invented for the graph -- it is already the
shared vocabulary of the flat-file contract, the analyst KG's data_series nodes, and the
SUMIFS criteria embedded in the balance-sheet formulas.

Two honest limits, both recorded in node properties rather than papered over:
  * SELECT DISTINCT is exact for what is IN the table and silent about what should be;
  * relations too large or too wide to enumerate are marked, not skipped quietly. A series
    that exists but was never enumerated must not look like a series that does not exist.
"""

from __future__ import annotations

# Precedence matters: the key is built in this order so `silver.monthly_realized[...]`
# reads the way a person would write it. Chosen from the catalog census -- these are the
# columns that actually carry a series identity, as opposed to a dimension (country, state,
# date) or a measure (value, quantity).
KEY_COLUMNS = [
    "commodity", "commodity_code", "class", "category",
    "attribute", "attribute_name", "series", "series_id", "series_name",
    "metric", "measure", "indicator", "variable",
    "fuel", "fuel_type", "feedstock", "product", "data_type",
    "source",
]

# Above this many distinct combinations we record the relation as series-bearing and stop.
# Enumerating 40k series from one trade table would swamp the graph without answering a
# question anyone is asking.
MAX_SERIES_PER_RELATION = 400

# reltuples above this and we do not even attempt the DISTINCT -- a seq scan over a
# multi-million-row table on every nightly scan is not worth the spine.
MAX_ROWS_FOR_ENUMERATION = 5_000_000

# reltuples is 0 for every view, so the row guard above cannot protect us from a DISTINCT
# over a gold view that joins four bronze tables. A statement timeout can, and it turns an
# unbounded hang into a recorded 'skipped_timeout' on the relation node.
STATEMENT_TIMEOUT = "20s"

# `source` alone is provenance, not identity -- a bronze table whose only key-ish column says
# 'EIA' on every row has one series, and enumerating it costs a scan to learn nothing. Require
# a real identity column, or at least two key columns together.
IDENTITY_COLUMNS = {
    "commodity", "commodity_code", "attribute", "attribute_name",
    "series", "series_id", "series_name", "metric", "measure",
    "indicator", "variable", "feedstock", "product",
}

CANDIDATE_SQL = """
SELECT c.relnamespace::regnamespace::text AS schema_name,
       c.relname                          AS relation_name,
       c.relkind                          AS relkind,
       c.reltuples::bigint                AS est_rows,
       array_agg(a.attname ORDER BY a.attnum) AS key_cols
  FROM pg_class c
  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
 WHERE c.relkind IN ('r','v','m','p')
   AND c.relnamespace::regnamespace::text NOT LIKE 'pg_%%'
   AND c.relnamespace::regnamespace::text NOT IN ('information_schema')
   AND a.attname = ANY(%(keycols)s)
 GROUP BY 1,2,3,4
"""


def series_key(relation: str, pairs: list[tuple[str, str]]) -> str:
    inner = ",".join(f"{k}={v}" for k, v in pairs)
    return f"{relation}[{inner}]"


def extract(conn, store, scope_schemas: tuple[str, ...] = ("bronze", "silver", "gold", "core", "reference"),
            verbose: bool = True) -> dict:
    cur = conn.cursor()
    cur.execute(CANDIDATE_SQL, {"keycols": KEY_COLUMNS})
    candidates = [r for r in cur.fetchall() if r["schema_name"] in scope_schemas]

    stats = {
        "relations_with_key_cols": len(candidates),
        "relations_enumerated": 0,
        "relations_too_large": 0,
        "relations_too_many_series": 0,
        "relations_failed": 0,
        "relations_timed_out": 0,
        "relations_provenance_only": 0,
        "series_nodes": 0,
        "key_column_nodes": 0,
    }

    for i, row in enumerate(candidates, 1):
        if verbose and i % 25 == 0:
            print(f"    [series] {i}/{len(candidates)}  series so far: {stats['series_nodes']}", flush=True)
        rel = f"{row['schema_name']}.{row['relation_name']}"
        if not store.has_node(rel):
            # Catalog extractor should already have created it; if not, the two disagree and
            # check 8.1 will say so. Do not invent the node here.
            continue

        cols = [c for c in KEY_COLUMNS if c in row["key_cols"]]
        for col in cols:
            store.add_node(
                "db_column", f"{rel}.{col}", label=f"{rel}.{col}",
                properties={"relation": rel, "role": "series_key"},
                extraction_method="pg_catalog", confidence=1.00,
            )
            store.add_edge(rel, "DEFINES", f"{rel}.{col}",
                           extraction_method="pg_catalog", confidence=1.00)
            stats["key_column_nodes"] += 1

        est = row["est_rows"] or 0
        if est > MAX_ROWS_FOR_ENUMERATION:
            stats["relations_too_large"] += 1
            _mark(store, rel, "skipped_too_large", cols, est)
            continue

        if not (set(cols) & IDENTITY_COLUMNS) and len(cols) < 2:
            stats["relations_provenance_only"] = stats.get("relations_provenance_only", 0) + 1
            _mark(store, rel, "skipped_provenance_only", cols, est)
            continue

        collist = ", ".join(f'"{c}"' for c in cols)
        sql = (
            f"SELECT DISTINCT {collist} FROM {row['schema_name']}.\"{row['relation_name']}\" "
            f"LIMIT {MAX_SERIES_PER_RELATION + 1}"
        )
        try:
            cur.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
            cur.execute(sql)
            combos = cur.fetchall()
        except Exception as exc:  # noqa: BLE001 -- a broken or slow view must not kill the scan
            conn.rollback()
            reason = "skipped_timeout" if "timeout" in str(exc).lower() else f"failed: {type(exc).__name__}"
            stats["relations_timed_out" if reason == "skipped_timeout" else "relations_failed"] = (
                stats.get("relations_timed_out" if reason == "skipped_timeout" else "relations_failed", 0) + 1
            )
            _mark(store, rel, reason, cols, est)
            if verbose:
                print(f"    [series] {rel}: {reason}", flush=True)
            continue

        if len(combos) > MAX_SERIES_PER_RELATION:
            stats["relations_too_many_series"] += 1
            _mark(store, rel, "skipped_too_many", cols, est, found=len(combos))
            continue

        for combo in combos:
            pairs = [(c, str(combo[c])) for c in cols if combo[c] is not None]
            if not pairs:
                continue
            key = series_key(rel, pairs)
            store.add_node(
                "data_series", key, label=key,
                properties={"relation": rel, "keys": dict(pairs)},
                extraction_method="pg_catalog", confidence=1.00,
            )
            store.add_edge(rel, "HAS_SERIES", key,
                           extraction_method="pg_catalog", confidence=1.00)
            stats["series_nodes"] += 1

        stats["relations_enumerated"] += 1
        _mark(store, rel, "enumerated", cols, est, found=len(combos))

    return stats


def _mark(store, rel: str, status: str, cols: list[str], est_rows: int, found: int | None = None):
    """Record on the relation node what we did and did not enumerate. A relation whose series
    were never listed must not be indistinguishable from one that genuinely has none."""
    props = {"series_enumeration": status, "series_key_columns": cols, "est_rows": est_rows}
    if found is not None:
        props["series_found"] = found
    store.add_node("db_relation", rel, properties=props,
                   extraction_method="pg_catalog", confidence=1.00)
